from boto3.dynamodb.conditions import Attr

def update_users_followers(username, following, table, remove=False):
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


def follow_user(username, user_id, table):
    item = table.get_item(Key={'username': username})['Item']
    new_follow = set([user_id]) - set(item['follow']) - set([username]) 
    new_item = table.update_item(
        Key={
            'username': username
        },
        UpdateExpression='SET follow = list_append(follow, :val1), follow_count = follow_count + :val2',
        ExpressionAttributeValues={
            ':val1': list(new_follow),
            ':val2': len(new_follow)
        },
        ReturnValues="UPDATED_NEW"
    )
    update_users_followers(username, set(new_follow), table, remove=False)
    # update_user_real_follow_count(username)


def create_user(update, table):
    username = str(update['message']['chat']['id'])
    followers = table.scan(
        FilterExpression=Attr('follow').contains(update['message']['chat']['username'])  
    )
    table.put_item(
        Item={
                'username': username,
                'first_name':  update.message.from_user.first_name.upper(),
                'last_name':  update.message.from_user.last_name.upper(),
                'follow': [],
                'follow_count': 0,
                'followers': [x['username'] for x in followers['Items']],
                'photo_id': 0
            }
        )


def update_user(update, table):
    username = str(update['message']['chat']['id'])
    followers = table.scan(
        FilterExpression=Attr('follow').contains(update['message']['chat']['username'])  
    )
    item = table.get_item(Key={'username': username})['Item']
    print(item)
    item['first_name'] = update.message.from_user.first_name.upper()
    item['last_name'] = update.message.from_user.last_name.upper()
    item['follow_count'] = len(item['follow'])
    item['followers'] = [x['username'] for x in followers['Items']]
    item['photo_id'] = item.get('photo_id', 0)
    table.put_item(Item=item)


def update_user_photo(photo, username, table):
    table.update_item(
            Key={
                'username': username
            },
            UpdateExpression='SET photo_id = :val1',
            ExpressionAttributeValues={
                ':val1': photo[-1]['file_id'],
            },
        )
