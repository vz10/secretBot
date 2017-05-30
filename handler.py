'''
Main handler for all the incoming bot events
'''
import logging
from telegram.ext import Updater, MessageHandler
import boto3
from boto3.dynamodb.conditions import Key, Attr


import config


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
dynamodb = boto3.resource('dynamodb', endpoint_url=config.DB_HOST)

COMMANDS = {
            'start', 
            'add',
            'remove'
            }

def message_handler(update):
    '''
    Handler for the text messages
    '''
    logger.info('message_handler')
    pass


def start_command_handler(update):
    '''
    Handler for the "start" command.
    Add current user to the Users table
    '''
    table = dynamodb.Table('users')
    followers = table.scan(
        FilterExpression=Attr('follow').contains(update['message']['chat']['username'])  
    )
    table.put_item(
        Item={
                'username': update['message']['chat']['username'],
                'user_id': update['message']['chat']['id'],
                'follow': [],
                'follow_count': 0,
                'real_follow_count': 0,
                'followers': [x['username'] for x in followers['Items']],
                'followers_count': followers['Count']
            }
        )
    logger.info('start_command_handler')


def add_command_handler(update):
    '''
    Handler for the "add" commands
    Add new user(s) to the current user following list
    '''
    users = list(map(lambda x: x[1:] if x.startswith('@') else x, 
                     update['message']['text'][4:].split()))
    logger.info('add_command_handler')


def remove_command_handler(update):
    '''
    Handler for the "remove" commands
    Remove new user(s) from the current user following list
    '''
    logger.info('remove_command_handler')
    pass


def dispatcher(_, update):
    for command in COMMANDS:
        if update['message']['text'].startswith(f'/{command}'):
            globals()[f'{command}_command_handler'](update)
            break
    else:
        message_handler(update)



def echo(bot, update):
    logger.info(update)
    update.message.reply_text(update.message.text)



def main():
    # Create the EventHandler and pass it your bot's token.
    updater = Updater(config.BOT_TOKEN)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # on noncommand i.e message - echo the message on Telegram
    dp.add_handler(MessageHandler(None, dispatcher))

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()