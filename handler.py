'''
Main handler for all the incoming bot events
'''
import logging
import telegram
from telegram.ext import Updater, MessageHandler, CommandHandler, Filters
from telegram.ext.inlinequeryhandler import InlineQueryHandler
from telegram.ext.choseninlineresulthandler import ChosenInlineResultHandler
import boto3
from boto3.dynamodb.conditions import Key, Attr

import config
from consts import RESPONSES, COMMANDS


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a connection to the database
dynamodb = boto3.resource('dynamodb', region_name=config.DB_REGION)
# dynamodb = boto3.resource('dynamodb', endpoint_url=config.DB_HOST)

table = dynamodb.Table(config.DB_NAME)

# # Create a bot instance for answering to the user
# bot = telegram.Bot(config.BOT_TOKEN)


def update_user_real_follow_count(username, follow=None):
    '''
    Update %username% real_follow_count after user add/remove
    somebody from their follow list
    '''
    if not follow:
        follow = table.get_item(Key={'username': username})['Item']['follow']
    new_real_follow_count = table.scan(FilterExpression=Attr('username').is_in(follow))['Count']
    table.update_item(
        Key={
            'username': username
        },
        UpdateExpression='SET real_follow_count = :val1',
        ExpressionAttributeValues={
            ':val1': new_real_follow_count,
        },
    )


def increase_users_real_follow_count(username):
    '''
    Find all the users that already have followed the new user 
    and increase their "real_follow_count" amount
    '''
    users_to_update = table.scan(FilterExpression=Attr('follow').contains(username))['Items']
    for user in users_to_update:
        table.update_item(
            Key={'username': user['username']},
            UpdateExpression='SET real_follow_count = real_follow_count + :val1',
            ExpressionAttributeValues={
            ':val1': 1,
            },
        )


def update_users_followers(username, following, remove=False):
    '''
    Find all the users that %username% follows and 
    update their "followers" list and "followers_count" amount
    '''

    for user in following:
        item = table.get_item(Key={'username': user}).get('Item', False)
        if item:
            try: 
                item['followers'].remove(username) if remove else item['followers'].append(username)
            except ValueError:
                continue
        else:
            continue 
        table.update_item(
            Key={
                'username': user
            },
            UpdateExpression='SET followers = :val1',
            ExpressionAttributeValues={
                ':val1': item['followers'],
            },
        )


def follow_user(username, user_id):
    item = table.get_item(Key={'username': username})['Item']
    new_follow = set([user_id]) - set(item['follow']) - set([username]) 
    new_item = table.update_item(
        Key={
            'username': username
        },
        UpdateExpression='SET follow = list_append(follow, :val1)',
        ExpressionAttributeValues={
            ':val1': list(new_follow),
        },
        ReturnValues="UPDATED_NEW"
    )
    update_users_followers(username, set(new_follow), remove=False)
    update_user_real_follow_count(username)



def message_handler(bot, update):
    '''
    Handler for the text messages
    '''
    logger.info(update)
    logger.info('message_handler')
    pass


def contact_handler(bot, update):
    '''
    Handler for the messages with contacts
    '''
    username = str(update['message']['chat']['id'])
    if  not update['message']['contact']['user_id']:
        bot.send_message(username, RESPONSES['empty_contact'])
        return
    
    follow_user(username, str(update['message']['contact']['user_id']))


def start_command_handler(bot, update):
    '''
    Handler for the "start" command.
    Add current user to the Users table
    '''

    # Avoid duplication of the existing users
    username = str(update['message']['chat']['id'])
    if table.get_item(Key={'username': username}).get('Item', False):
        return 

    followers = table.scan(
        FilterExpression=Attr('follow').contains(update['message']['chat']['username'])  
    )
    table.put_item(
        Item={
                'username': username,
                'first_name':  update.message.from_user.first_name.upper(),
                'last_name':  update.message.from_user.last_name.upper(),
                'user_id': username,
                'follow': [],
                'real_follow_count': 0,
                'followers': [x['username'] for x in followers['Items']],
                'followers_count': followers['Count']
            }
        )
    increase_users_real_follow_count(update['message']['chat']['username'])
    logger.info('start_command_handler')


def add_command_handler(bot, update):
    '''
    Handler for the "add" commands
    Add new user(s) to the current user following list
    '''
    users = list(map(lambda x: x[1:] if x.startswith('@') else x, 
                     update['message']['text'][len('/add'):].split()))
    chat_id = update['message']['chat']['id']
    username = str(update['message']['chat']['id'])
    if not users:
        bot.send_message(chat_id, RESPONSES['empty_add_command'])
        return

    item = table.get_item(Key={'username': username})['Item']
    new_follow = set(users) - set(item['follow']) - set([username]) 
    new_item = table.update_item(
        Key={
            'username': username
        },
        UpdateExpression='SET follow = list_append(follow, :val1)',
        ExpressionAttributeValues={
            ':val1': list(new_follow),
        },
        ReturnValues="UPDATED_NEW"
    )
    update_users_followers(username, set(new_follow), remove=False)
    update_user_real_follow_count(username)

    logger.info('add_command_handler')


def remove_command_handler(bot, update):
    '''
    Handler for the "remove" commands
    Remove user(s) from the current user following list
    '''
    users = list(map(lambda x: x[1:] if x.startswith('@') else x, 
                     update['message']['text'][len('/remove'):].split()))
    chat_id = update['message']['chat']['id']
    username = str(update['message']['chat']['id'])
    if not users:
        bot.send_message(chat_id, RESPONSES['empty_remove_command'])
        return

    item = table.get_item(Key={'username': username})['Item']
    new_follow = set(item['follow']) - set(users)
    table.update_item(
        Key={
            'username': username
        },
        UpdateExpression='SET follow = :val1',
        ExpressionAttributeValues={
            ':val1': list(new_follow),
        }
    )

    update_users_followers(username, set(users), remove=True)
    update_user_real_follow_count(username, follow=new_follow)

    logger.info('remove_command_handler')


def send_command_handler(bot, update):
    '''
    Handler for the "send" command
    Send message to all the followers who has more that 10 real_following
    '''
    message = update['message']['text'][len('/send'):]
    if not message:
        chat_id = update['message']['chat']['id']
        bot.send_message(chat_id, RESPONSES['empty_send_command'])
        return 

    username = str(update['message']['chat']['id'])

    users_to_send = table.scan(FilterExpression=Attr('follow').contains(username))['Items']
    for user in users_to_send:
        bot.send_message(int(user['user_id']), f'Somebody told me, that "{message}"')
    logger.info('send_command_handler')


def photo_handler(bot, update):
    '''
    Handler for the photo messages
    Send message with photo to all the followers who has more that 10 real_following
    '''
    photo = update['message']['photo']
    username = str(update['message']['chat']['id'])

    users_to_send = table.scan(FilterExpression=Attr('follow').contains(username))['Items']
    for user in users_to_send:
        bot.send_photo(int(user['user_id']), photo=photo[-1]['file_id'], caption='Somebody has just shown me that')
    logger.info('send_photo_handler')


def document_handler(bot, update):
    '''
    Handler for the docume t messages
    Send message with photo to all the followers who has more that 10 real_following
    '''
    document = update['message']['document']
    username = str(update['message']['chat']['id'])

    users_to_send = table.scan(FilterExpression=Attr('follow').contains(username))['Items']
    for user in users_to_send:
        bot.send_document(int(user['user_id']), document=document['file_id'])
    logger.info('send_document_handler')


def inline_query_handler(bot, update):
    query = update.inline_query.query
    inline_query_id = update.inline_query.id

    if len(query) < 3:
        bot.answerInlineQuery(inline_query_id, [])
        return

    query_result = table.scan(FilterExpression=Attr('first_name').contains(query.upper()) | Attr('last_name').contains(query.upper()))['Items']
    query_articles = list(map(lambda x: telegram.InlineQueryResultArticle(x['username'], 
                                                                          '%s %s' % (x['first_name'] or '', x['last_name'] or ''),
                                                                          telegram.InputTextMessageContent('%s %s' % (x['first_name'], x['last_name']))),
                                                                          query_result))
    bot.answerInlineQuery(inline_query_id, query_articles)


def inline_query_result_handler(bot, update):
    logger.info("QUERY RESULT UPDATE %s %s" % (str(update.chosen_inline_result.from_user.id), str(update.chosen_inline_result.result_id)))
    follow_user(str(update.chosen_inline_result.from_user.id), str(update.chosen_inline_result.result_id))
    logger.info('Query result handler')


def echo(bot, update):
    logger.info(update)
    update.message.reply_text(update.message.text)


def lambda_handler(event, context):
    logger.info(f'HANDLER UPDATE {event} {context}')
    updater = Updater(config.BOT_TOKEN)
    bot = telegram.Bot(config.BOT_TOKEN)

    dp = updater.dispatcher
    register_handlers(dp)

    dp.process_update(telegram.Update.de_json(event, bot))


def main():
    # Create the EventHandler and pass it your bot's token.
    updater = Updater(config.BOT_TOKEN)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # on noncommand i.e message - echo the message on Telegram
    # dp.add_handler(MessageHandler(None, dispatcher))

    register_handlers(dp)

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


def register_handlers(dp):
    dp.add_handler(CommandHandler('start', start_command_handler))
    dp.add_handler(CommandHandler('add', add_command_handler))
    dp.add_handler(CommandHandler('send', send_command_handler))
    dp.add_handler(CommandHandler('remove', remove_command_handler))
    dp.add_handler(MessageHandler(Filters.contact, contact_handler))
    dp.add_handler(MessageHandler(Filters.photo, photo_handler))
    dp.add_handler(MessageHandler(Filters.document, document_handler))
    dp.add_handler(InlineQueryHandler(inline_query_handler))
    dp.add_handler(ChosenInlineResultHandler(inline_query_result_handler))


if __name__ == '__main__':
    main()
