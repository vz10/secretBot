'''
Main handler for all the incoming bot events
'''
import logging
from concurrent.futures import ThreadPoolExecutor

import telegram
from telegram.ext import Updater, MessageHandler, CommandHandler, Filters
from telegram.ext.inlinequeryhandler import InlineQueryHandler
from telegram.ext.choseninlineresulthandler import ChosenInlineResultHandler
import boto3
from boto3.dynamodb.conditions import Key, Attr

import config
from consts import RESPONSES, COMMANDS
from db_actions import update_users_followers, follow_user, create_user, update_user_photo

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a connection to the database
dynamodb = boto3.resource('dynamodb', region_name=config.DB_REGION)
# dynamodb = boto3.resource('dynamodb', endpoint_url=config.DB_HOST)

table = dynamodb.Table(config.DB_NAME)


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
    # if table.get_item(Key={'username': username}).get('Item', False):
    #     return 
    create_user(update, table)

    photo = bot.getUserProfilePhotos(update.message.from_user.id)['photos'][0]
    update_user_photo(photo, username, table)

    logger.info('start_command_handler')


# def add_command_handler(bot, update):
#     '''
#     Handler for the "add" commands
#     Add new user(s) to the current user following list
#     '''
#     users = list(map(lambda x: x[1:] if x.startswith('@') else x, 
#                      update['message']['text'][len('/add'):].split()))
#     chat_id = update['message']['chat']['id']
#     username = str(update['message']['chat']['id'])
#     if not users:
#         bot.send_message(chat_id, RESPONSES['empty_add_command'])
#         return

#     item = table.get_item(Key={'username': username})['Item']
#     new_follow = set(users) - set(item['follow']) - set([username]) 
#     new_item = table.update_item(
#         Key={
#             'username': username
#         },
#         UpdateExpression='SET follow = list_append(follow, :val1)',
#         ExpressionAttributeValues={
#             ':val1': list(new_follow),
#         },
#         ReturnValues="UPDATED_NEW"
#     )
#     update_users_followers(username, set(new_follow), remove=False)
#     # update_user_real_follow_count(username)

#     logger.info('add_command_handler')


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

    update_users_followers(username, set(users), table, remove=True)
    # update_user_real_follow_count(username, follow=new_follow)

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
    message_to_send = f'Somebody told me, that "{message}"'
    with ThreadPoolExecutor(max_workers=min(len(users_to_send), 10)) as Executor:
        list(Executor.map(lambda x: bot.send_message(*x), 
                          [(int(user['username']), message_to_send) 
                           for user in users_to_send]))
    # for user in users_to_send:
        # bot.send_message(int(user['username']), f'Somebody told me, that "{message}"')
    logger.info('send_command_handler')


def photo_handler(bot, update):
    '''
    Handler for the photo messages
    Send message with photo to all the followers who has more that 10 real_following
    '''
    photo = update['message']['photo']
    username = str(update['message']['chat']['id'])

    users_to_send = table.scan(FilterExpression=Attr('follow').contains(username))['Items']
    photo_to_send = photo[-1]['file_id']
    caption='Somebody has just shown me that'
    with ThreadPoolExecutor(max_workers=min(len(users_to_send), 10)) as Executor:
        list(Executor.map(lambda x: bot.send_photo(*x), 
                          [(int(user['username']), photo_to_send, caption) 
                           for user in users_to_send]))
    logger.info('send_photo_handler')


def document_handler(bot, update):
    '''
    Handler for the docume t messages
    Send message with photo to all the followers who has more that 10 real_following
    '''
    document = update['message']['document']
    username = str(update['message']['chat']['id'])

    users_to_send = table.scan(FilterExpression=Attr('follow').contains(username))['Items']
    document=document['file_id']
    with ThreadPoolExecutor(max_workers=min(len(users_to_send), 10)) as Executor:
        list(Executor.map(lambda x: bot.send_document(*x), 
                          [(int(user['username']), document) 
                          for user in users_to_send]))
    logger.info('send_document_handler')


def inline_query_handler(bot, update):
    query = update.inline_query.query
    inline_query_id = update.inline_query.id

    if len(query) < 3:
        bot.answerInlineQuery(inline_query_id, [])
        return

    query_result = table.scan(FilterExpression=Attr('first_name').contains(query.upper()) | 
                                               Attr('last_name').contains(query.upper()))['Items']
    query_articles = list(map(lambda x: telegram.InlineQueryResultArticle(x['username'], 
                                                                          '%s %s' % (x['first_name'] or '', x['last_name'] or ''),
                                                                          telegram.InputTextMessageContent('%s %s' % (x['first_name'], x['last_name']))),
                                                                          query_result))
    bot.answerInlineQuery(inline_query_id, query_articles)


def inline_query_result_handler(bot, update):
    logger.info("QUERY RESULT UPDATE %s %s" % (str(update.chosen_inline_result.from_user.id), str(update.chosen_inline_result.result_id)))
    follow_user(str(update.chosen_inline_result.from_user.id), str(update.chosen_inline_result.result_id))
    logger.info('Query result handler')


def lambda_handler(event, context):
    logger.info(f'HANDLER UPDATE {event} {context}')
    updater = Updater(config.BOT_TOKEN)
    bot = telegram.Bot(config.BOT_TOKEN)

    dp = updater.dispatcher
    register_handlers(dp)

    dp.process_update(telegram.Update.de_json(event, bot))


def main():
    updater = Updater(config.BOT_TOKEN)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher
    register_handlers(dp)
    updater.start_polling()

    updater.idle()


def register_handlers(dp):
    dp.add_handler(CommandHandler('start', start_command_handler))
    # dp.add_handler(CommandHandler('add', add_command_handler))
    dp.add_handler(CommandHandler('send', send_command_handler))
    dp.add_handler(CommandHandler('remove', remove_command_handler))
    dp.add_handler(MessageHandler(Filters.contact, contact_handler))
    dp.add_handler(MessageHandler(Filters.photo, photo_handler))
    dp.add_handler(MessageHandler(Filters.document, document_handler))
    dp.add_handler(InlineQueryHandler(inline_query_handler))
    dp.add_handler(ChosenInlineResultHandler(inline_query_result_handler))


if __name__ == '__main__':
    main()
