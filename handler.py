'''
Main handler for all the incoming bot events
'''
import logging
from concurrent.futures import ThreadPoolExecutor

import telegram
from telegram.ext import Updater, MessageHandler, CommandHandler, Filters, CallbackQueryHandler
from telegram.ext.inlinequeryhandler import InlineQueryHandler
from telegram.ext.choseninlineresulthandler import ChosenInlineResultHandler
import boto3
from boto3.dynamodb.conditions import Key, Attr

import config
from consts import RESPONSES, COMMANDS
from db_actions import (update_users_followers, follow_user, unfollow_user,
                        create_user, update_user_photo, update_user, get_followers_list)

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a connection to the database
table = None

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
    user_to_follow = str(update['message']['contact']['user_id'])
    if  not update['message']['contact']['user_id']:
        bot.send_message(username, RESPONSES['empty_contact'])
        return
    
    new_follower = follow_user(username, user_to_follow, table)
    if new_follower:
        bot.send_message(user_to_follow, RESPONSES['new_follower'])
    logger.info('contat_handler')

def start_command_handler(bot, update):
    '''
    Handler for the "start" command.
    Add current user to the Users table
    '''

    # Avoid duplication of the existing users
    username = str(update['message']['chat']['id'])
    create_user(update, table)

    photo = bot.getUserProfilePhotos(update.message.from_user.id)['photos'][0]
    update_user_photo(photo, username, table)

    logger.info('start_command_handler')


def update_command_handler(bot, update):
    '''
    Handler for the "update" commands
    Update user info in database
    '''
    username = str(update['message']['chat']['id'])
    update_user(update, table)

    photo = bot.getUserProfilePhotos(update.message.from_user.id)['photos'][0]
    update_user_photo(photo, username, table)

    logger.info('update_command_handler')


def send_all_command_handler(bot, update):
    username = str(update['message']['chat']['id'])
    if not username == config.MAIN_USER:
        return
    all_users = table.scan()['Items']
    message = update['message']['text'][len('/send_all'):]

    with ThreadPoolExecutor(max_workers=min(len(all_users), config.MAX_THREADS)) as Executor:
        list(Executor.map(lambda x: bot.send_message(*x), 
                          [(int(user['username']), RESPONSES['important_message'].format(message)) 
                           for user in all_users]))


def remove_command_handler(bot, update):
    '''
    Handler for the "remove" commands
    Remove user(s) from the current user following list
    '''
    chat_id = update['message']['chat']['id']
    username = str(update['message']['chat']['id'])
    users = get_followers_list(username, table)
    if not users:
        bot.send_message(chat_id, RESPONSES['empty_remove_command'])
        return
    logger.info(users)
    buttons = [telegram.InlineKeyboardButton(text='%s %s' % (user.get('first_name', ''), user.get('last_name', '')),
                                             callback_data=str(user['username'])) for user in users]
    reply_markup = telegram.InlineKeyboardMarkup([[button] for button in buttons])
    bot.sendMessage(chat_id=chat_id,
                    text=RESPONSES['remove_keyboard_message'],
                    reply_markup=reply_markup)

    logger.info('remove_command_handler')


def remove_user_callback(bot, update):
    '''
    Handler callback from custom keyboard for the "remove" commands
    Remove user from the current user following list
    '''
    logger.info('='*80)
    username = str(update['callback_query']['message']['chat']['id'])
    unfollower_id = str(update['callback_query']['data'])
    logger.info("remove users %s %s" % (username, unfollower_id))
    # update_user_real_follow_count(username, follow=new_follow)
    unfollow_user(username, unfollower_id, table)


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
    if not users_to_send:
        return

    with ThreadPoolExecutor(max_workers=min(len(users_to_send), config.MAX_THREADS)) as Executor:
        list(Executor.map(lambda x: bot.send_message(*x), 
                          [(int(user['username']), RESPONSES['message_boilerplate'].format(message)) 
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
    if not users_to_send:
        return
    photo_to_send = photo[-1]['file_id']
    with ThreadPoolExecutor(max_workers=min(len(users_to_send), config.MAX_THREADS)) as Executor:
        list(Executor.map(lambda x: bot.send_photo(*x), 
                          [(int(user['username']), photo_to_send, RESPONSES['photo_caption']) 
                           for user in users_to_send]))
    logger.info('send_photo_handler')


def document_handler(bot, update):
    '''
    Handler for the document messages
    Send message with photo to all the followers who has more that 10 real_following
    '''
    document = update['message']['document']['file_id']
    username = str(update['message']['chat']['id'])

    users_to_send = table.scan(FilterExpression=Attr('follow').contains(username))['Items']
    if not users_to_send:
        return
    with ThreadPoolExecutor(max_workers=min(len(users_to_send), config.MAX_THREADS)) as Executor:
        list(Executor.map(lambda x: bot.send_document(*x), 
                          [(int(user['username']), document) 
                          for user in users_to_send]))
    logger.info('send_document_handler')


def sticker_handler(bot, update):
    '''
    Handler for the sticker messages
    Send message with sticker to all the followers who has more that 10 real_following
    '''
    def send_message_and_sticker(chat_id):
        '''
        Just a little handler to be sure that sticker will be send
        after the message
        '''
        bot.send_message(chat_id, RESPONSES['before_sticker_send'])
        bot.send_sticker(chat_id, sticker)

    username = str(update['message']['chat']['id'])
    sticker = update['message']['sticker']['file_id']
    users_to_send = table.scan(FilterExpression=Attr('follow').contains(username))['Items']
    if not users_to_send:
        return
    
    with ThreadPoolExecutor(max_workers=min(len(users_to_send), config.MAX_THREADS)) as Executor:
        list(Executor.map(send_message_and_sticker, 
                          [int(user['username']) for user in users_to_send]))
    logger.info('send_sticker_handler')


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
    username = str(update.chosen_inline_result.from_user.id)
    user_to_follow = str(update.chosen_inline_result.result_id)
    new_follower = follow_user(username, user_to_follow, table)
    if new_follower:
        bot.send_message(user_to_follow, RESPONSES['new_follower'])
    logger.info('Query result handler')


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name=config.DB_REGION)
    globals()['table'] = dynamodb.Table(config.DB_NAME)

    logger.info(f'HANDLER UPDATE {event} {context}')
    updater = Updater(config.BOT_TOKEN)
    bot = telegram.Bot(config.BOT_TOKEN)

    dp = updater.dispatcher
    register_handlers(dp)

    dp.process_update(telegram.Update.de_json(event, bot))


def main():
    dynamodb = boto3.resource('dynamodb', endpoint_url=config.DB_HOST)
    globals()['table'] = dynamodb.Table(config.DB_NAME)

    updater = Updater(config.TEST_BOT_TOKEN)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher
    register_handlers(dp)
    updater.start_polling()

    updater.idle()


def register_handlers(dp):
    for command in COMMANDS:
        dp.add_handler(CommandHandler(command, globals()[f'{command}_command_handler']))
    dp.add_handler(MessageHandler(Filters.contact, contact_handler))
    dp.add_handler(MessageHandler(Filters.photo, photo_handler))
    dp.add_handler(MessageHandler(Filters.document, document_handler))
    dp.add_handler(MessageHandler(Filters.sticker, sticker_handler))
    dp.add_handler(InlineQueryHandler(inline_query_handler))
    dp.add_handler(ChosenInlineResultHandler(inline_query_result_handler))
    dp.add_handler(CallbackQueryHandler(remove_user_callback))


if __name__ == '__main__':
    main()
