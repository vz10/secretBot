'''
All the consts and hardcode are here
'''

RESPONSES = {
    'empty_add_command': 'There should be at least one Username to add. For example "/add @username" or "/add username".',
    'empty_remove_command': 'There should be at least one Username to remove. For example "/remove @username" or "/remove username".',
    'empty_send_command': 'There should be a message after "/send". For example "/send  I have a proof that there are reptilians in the government".',
    'empty_contact': 'The user is not in the Telegram yet.',
    'before_sticker_send': 'Someone has just gave me the sticker',
    'photo_caption': 'Somebody has just shown me that',
    'message_boilerplate': 'Somebody told me, that "{}"',
    'important_message': 'Important message for all the users - "{}"'
}

COMMANDS = {
    'start',
    'remove',
    'send',
    'update',
    'send_all'
}
