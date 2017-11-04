import sys
import copy
import csv
import datetime
import json
import socket
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

def addCommentsAndRepliesToCSV(comments, nodeoutfile, edgeoutfile):
    for comment in comments['data']:
        parent_comment_id = [ comment['id'] ]
        csv_data = []
        csv_data.insert(0, parent_comment_id)
        save_csv(nodeoutfile, csv_data, file_mode="a")
        if 'comments' in comment:
            for reply in comment['comments']['data']:
                list_of_user_in_reply = []
                if reply['from']['id'] not in list_of_user_in_reply:
                    list_of_user_in_reply.append(reply['from']['id'])
                    reply_id = [reply['from']['id']]
                    csv_data = []
                    csv_data.insert(0, reply_id)
                    save_csv(nodeoutfile, csv_data, file_mode="a")
                    #insertar las aristas
                    edge = [reply['from']['id'], comment['id']]
                    csv_data = []
                    csv_data.insert(0, edge)
                    save_csv(edgeoutfile, csv_data, file_mode="a")

def addPostsAndCommentsToCSV(post, outfile_nodes, outfile_edges):
    list_posts = [post['id']]
    csv_data = []
    csv_data.insert(0, list_posts)
    save_csv(outfile_nodes, csv_data, file_mode="a")
    if 'comments' in post:
        for comment in post['comments']['data']:
            list_of_user_in_post = []
            if comment['from']['id'] not in list_of_user_in_post:
                list_of_user_in_post.append(comment['from']['id'])
                list_comment_id = [comment['from']['id']]
                csv_data = []
                csv_data.insert(0, list_comment_id)
                save_csv(outfile_nodes, csv_data, file_mode="a")
                # insertar las aristas
                edge = [comment['from']['id'], post['id']]
                csv_data = []
                csv_data.insert(0, edge)
                save_csv(outfile_edges, csv_data, file_mode="a")


def getEngagement(reactions):
    return 1


def insertCommentData(post_id):
    return 0

def insertPostData(post):
    print(1)
    post_id = post['id']
    post_date = post['created_time']
    post_link = post['link']
    post_facebook_link = 'https://www.facebook.com/' + post_id
    post_title = post['name']
    post_engagement = getEngagement(post['reactions'])


def buildCommentsCSVs(client_id, client_secret, site_id, outfile_nodes, outfile_edges, version="2.10"):
    fb_token = getAccessToken(client_id, client_secret)
    since = '1506902400'
    until = '1508198400'
    field_list = 'id,message,created_time,comments{id,message,comments{id,message,from}}'

    data_url = 'https://graph.facebook.com/v' + version + '/' + site_id + '/posts?fields=' + field_list + '&limit=100&since='+since+'&until='+until+'&' + fb_token
    next_item = url_retry(data_url)

    for post in next_item['data']:
        if 'comments' in post:
            addCommentsAndRepliesToCSV(post['comments'], outfile_nodes, outfile_edges )

    while 'paging' in next_item and 'next' in next_item['paging']:
        next_item = url_retry(next_item['paging']['next'])
        for post in next_item['data']:
            if 'comments' in post:
                addCommentsAndRepliesToCSV(post['comments'], outfile_nodes, outfile_edges)



def addPosts(client_id, client_secret, site_id, site_name, since_date,until_date, version="2.10"):
    fb_token = getAccessToken(client_id, client_secret)
    since = '1506902400'
    until = '1508198400'
    field_list = 'id,name,created_time,link,comments{id,from},reactions'
    data_url = 'https://graph.facebook.com/v' + version + '/' + site_id + '/posts?fields=' + field_list + '&limit=100&since='+since+'&until='+until+'&' + fb_token
    next_item = url_retry(data_url)


    for post in next_item['data']:
        insertPostData(post)

    while 'paging' in next_item and 'next' in next_item['paging']:
        next_item = url_retry(next_item['paging']['next'])
        #for post in next_item['data']:
            #addPostsAndCommentsToCSV(post, outfile_nodes, outfile_edges)




addPosts("264737207353432","460c5a58dd6ddd6997b2645b1ad37cdd","115872105050", "", "","","2.10")

#115872105050?fields=posts{id,created_time,name}&limit=100
