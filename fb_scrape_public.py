from Neo4JQueryBuilder import *
import copy
import csv
import json
import time
import urllib.request

def load_data(data, enc='utf-8'):
    if type(data) is str:
        csv_data = []
        with open(data, 'r', encoding=enc, errors='replace') as f:
            reader = csv.reader((line.replace('\0', '') for line in f))  # remove NULL bytes
            for row in reader:
                if row != []:
                    csv_data.append(row)
        return csv_data
    else:
        return copy.deepcopy(data)


def save_csv(filename, data, use_quotes=True, file_mode='w',
             enc='utf-8'):  # this assumes a list of lists wherein the second-level list items contain no commas
    with open(filename, file_mode, encoding=enc) as out:
        for line in data:
            if use_quotes == True:
                row = '"' + '","'.join([str(i).replace('"', "'") for i in line]) + '"' + "\n"
            else:
                row = ','.join([str(i) for i in line]) + "\n"
            out.write(row)


def url_retry(url):
    succ = 0
    while succ == 0:
        try:
            json_out = json.loads(urllib.request.urlopen(url).read().decode(encoding="utf-8"))
            succ = 1
        except Exception as e:
            print(str(e))
            if 'HTTP Error 4' in str(e):
                return False
            else:
                time.sleep(1)
    return json_out


def optional_field(dict_item, dict_key):
    try:
        out = dict_item[dict_key]
        if dict_key == 'shares':
            out = dict_item[dict_key]['count']
        if dict_key == 'likes':
            out = dict_item[dict_key]['summary']['total_count']
    except KeyError:
        out = ''
    return out


#Métodos propios------------------------------------------------------------------------

def getAccessToken(client_id, client_secret):
    fb_urlobj = urllib.request.urlopen('https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id=' + client_id + '&client_secret=' + client_secret)
    fb_token = 'access_token=' + json.loads(fb_urlobj.read().decode(encoding="latin1"))['access_token']
    return fb_token

def getReactionRelatedData(reaction, reacted_object_id, reacted_object_label):
    queryList = []
    attributeList = [('name', reaction['name'])]
    user_node_insertion_query = buildInsertOrUpdateNodeQuery('User', reaction['id'], attributeList)
    attributeList = [('type', reaction['type'])]
    reaction_user_relationship_query = buildInsertOrUpdateRelationshipQuery('REACTS_TO', 'User', reaction['id'], \
                                                                            reacted_object_label, reacted_object_id, attributeList)
    queryList.extend([user_node_insertion_query,reaction_user_relationship_query])
    return queryList

def getReplyRelatedData(reply, comment_id, post_id):
    queryList = []
    attributeList = [('date', reply['created_time'])]
    reply_node_insertion_query = buildInsertOrUpdateNodeQuery('Comment',reply['id'],attributeList)
    attributeList = []
    post_comment_relationship_query = buildInsertOrUpdateRelationshipQuery('BELONGS_TO', 'Comment', reply['id'], 'Post', post_id, attributeList)
    comment_reply_relationship_query= buildInsertOrUpdateRelationshipQuery('REPLIES_TO', 'Comment', reply['id'], 'Comment', comment_id, attributeList)
    attributeList = [('name', reply['from']['name'])]
    user_node_insertion_query = buildInsertOrUpdateNodeQuery('User', reply['from']['id'], attributeList)
    attributeList = []
    reply_user_relationship_query = buildInsertOrUpdateRelationshipQuery('POSTED', 'User', reply['from']['id'],'Comment',reply['id'],attributeList)
    queryList.extend([reply_node_insertion_query,post_comment_relationship_query,comment_reply_relationship_query,user_node_insertion_query,reply_user_relationship_query])
    if 'reactions' in reply:
        for reaction in reply['reactions']['data']:
            queryList.extend(getReactionRelatedData(reaction, reply['id'], 'Comment'))
        while 'paging' in reply['reactions'] and 'next' in reply['reactions']['paging']:
            reply['reactions'] = url_retry(reply['reactions']['paging']['next'])
            for reaction in reply['reactions']['data']:
                queryList.extend(getReactionRelatedData(reaction, reply['id'], 'Comment'))
    return queryList

def getCommentRelatedData(comment, post_id):
    queryList = []
    attributeList = [('date',comment['created_time']),('text', comment['message'] )]
    comment_node_insertion_query = buildInsertOrUpdateNodeQuery('Comment',comment['id'], attributeList)
    attributeList = []
    post_comment_relationship_query = buildInsertOrUpdateRelationshipQuery('BELONGS_TO', 'Comment', comment['id'], 'Post', post_id, attributeList)
    attributeList = [('name', comment['from']['name'])]
    user_node_insertion_query = buildInsertOrUpdateNodeQuery('User', comment['from']['id'], attributeList)
    attributeList = []
    comment_user_relationship_query = buildInsertOrUpdateRelationshipQuery('POSTED', 'User', comment['from']['id'],'Comment',comment['id'],attributeList)
    queryList.extend([comment_node_insertion_query,post_comment_relationship_query,user_node_insertion_query,comment_user_relationship_query])
    if 'comments' in comment:
        for reply in comment['comments']['data']:
            queryList.extend(getReplyRelatedData(reply, comment['id'], post_id))
        while 'paging' in comment['comments'] and 'next' in comment['comments']['paging']:
            comment['comments'] = url_retry(comment['comments']['paging']['next'])
            for reply in comment['comments']['data']:
                queryList.extend(getReplyRelatedData(reply, comment['id'],post_id))

    if 'reactions' in comment:
        for reaction in comment['reactions']['data']:
            queryList.extend(getReactionRelatedData(reaction,comment['id'],'Comment'))
            while 'paging' in comment['reactions'] and 'next' in comment['reactions']['paging']:
                comment['reactions']  = url_retry(comment['reactions']['paging']['next'])
                for reaction in comment['reactions']['data']:
                    queryList.extend(getReactionRelatedData(reaction, comment['id'], 'Comment'))
    return queryList

def getPostRelatedData(post, site_name):
    queryList = []
    post_id = post['id']
    post_date = post['created_time']
    post_link = post['link']
    post_facebook_link = 'https://www.facebook.com/' + post_id
    post_title = post['name'] if 'name' in post else 'Sin título'
    post_reaction_count = post['like']['summary']['total_count']  + post['love']['summary']['total_count'] +\
                          post['wow']['summary']['total_count']   + post['haha']['summary']['total_count'] +\
                          post['sad']['summary']['total_count']   + post['angry']['summary']['total_count']
    post_share_count = post['shares']['count'] if 'shares' in post else 0
    post_engagement = post_reaction_count + post_share_count
    attributeList = [('date',post_date),('link',post_link),\
                     ('fb_link',post_facebook_link),('title',post_title),\
                     ('share_count',post_share_count), ('site', site_name), \
                     ('reaction_count',post_reaction_count),('engagement',post_engagement)]
    post_node_insertion_query = buildInsertOrUpdateNodeQuery('Post',post_id, attributeList)
    queryList.append(post_node_insertion_query)
    if 'comments' in post:
        for comment in post['comments']['data']:
            queryList.extend(getCommentRelatedData(comment, post_id))
        while 'paging' in post['comments'] and 'next' in post['comments']['paging']:
            post['comments'] = url_retry(post['comments']['paging']['next'])
            for comment in post['comments']['data']:
                queryList.extend(getCommentRelatedData(comment, post_id))

    if 'reactions' in post:
        for reaction in post['reactions']['data']:
            queryList.extend(getReactionRelatedData(reaction,post_id,'Post'))
        while 'paging' in post['reactions'] and 'next' in post['reactions']['paging']:
            post['reactions'] = url_retry(post['reactions']['paging']['next'])
            for reaction in post['reactions']['data']:
                queryList.extend(getReactionRelatedData(reaction, post_id,'Post'))
    return queryList



def addPosts(client_id, client_secret, site_id, site_name, since_date,until_date, version="2.10"):
    queryList = []
    fb_token = getAccessToken(client_id, client_secret)
    since = '1506902400'
    until = '1508198400'
    reaction_count_queries = 'reactions.type(LIKE).limit(0).summary(1).as(like),reactions.type(WOW).limit(0).summary(1).as(wow),' \
                             'reactions.type(SAD).limit(0).summary(1).as(sad),reactions.type(HAHA).limit(0).summary(1).as(haha),' \
                             'reactions.type(LOVE).limit(0).summary(1).as(love),reactions.type(ANGRY).limit(0).summary(1).as(angry),'
    field_list = 'id,name,created_time,link,shares,comments{id,from,created_time,comments{id,from,created_time,reactions,message},reactions,message},'+reaction_count_queries+'reactions'
    data_url = 'https://graph.facebook.com/v' + version + '/' + site_id + '/posts?fields=' + field_list + '&limit=100&since='+since+'&until='+until+'&' + fb_token
    next_item = url_retry(data_url)

    for post in next_item['data']:
        queryList.extend(getPostRelatedData(post, site_name))

    print(1)

  # #while 'paging' in next_item and 'next' in next_item['paging']:
  #     next_item = url_retry(next_item['paging']['next'])
  #     for post in next_item['data']:
  #         getPostRelatedData(post, site_name)





addPosts("264737207353432","460c5a58dd6ddd6997b2645b1ad37cdd","115872105050", "", "","","2.10")

#115872105050?fields=posts{id,created_time,name}&limit=100
