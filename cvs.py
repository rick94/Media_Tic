import sys
import copy
import csv
import datetime
import json
import socket
import time
import urllib.request

socket.setdefaulttimeout(30)


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


def make_csv_chunk(fb_json_page, scrape_mode, thread_starter='', msg=''):
    csv_chunk = []
    if scrape_mode == 'feed' or scrape_mode == 'posts':
        for line in fb_json_page['data']:
            csv_line = [line['from']['name'], \
                        '_' + line['from']['id'], \
                        optional_field(line, 'message'), \
                        optional_field(line, 'picture'), \
                        optional_field(line, 'link'), \
                        optional_field(line, 'name'), \
                        optional_field(line, 'description'), \
                        optional_field(line, 'type'), \
                        line['created_time'], \
                        optional_field(line, 'shares'), \
                        optional_field(line, 'likes'), \
                        optional_field(line, 'LOVE'), \
                        optional_field(line, 'WOW'), \
                        optional_field(line, 'HAHA'), \
                        optional_field(line, 'SAD'), \
                        optional_field(line, 'ANGRY'), \
                        line['id']]
            csv_chunk.append(csv_line)
    if scrape_mode == 'comments':
        for line in fb_json_page['data']:
            csv_line = [line['from']['name'], \
                        '_' + line['from']['id'], \
                        optional_field(line, 'message'), \
                        line['created_time'], \
                        optional_field(line, 'like_count'), \
                        line['id'], \
                        thread_starter, \
                        msg]
            csv_chunk.append(csv_line)

    return csv_chunk


'''
# The first five fields of scrape_fb are fairly self-explanatory or are explained above. 
# scrape_mode can take three values: "feed," "posts," or "comments." The first two are identical in most cases and pull the main posts from a public wall. "comments" pulls the comments from a given permalink for a post. Only use "comments" if your IDs are post permalinks.
# You can use end_date to specify a date around which you'd like the program to stop. It won't stop exactly on that date, but rather a little after it. If present, it needs to be a string in yyyy-mm-dd format. If you leave the field blank, it will extract all available data. 
'''


def scrape_fb(client_id, client_secret, ids, outfile="fb_data.csv", version="2.7", scrape_mode="feed", end_date=""):
    time1 = time.time()
    if type(client_id) is int:
        client_id = str(client_id)
    fb_urlobj = urllib.request.urlopen(
        'https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id=' + client_id + '&client_secret=' + client_secret)
    fb_token = 'access_token=' + json.loads(fb_urlobj.read().decode(encoding="latin1"))['access_token']
    if "," in ids:
        fb_ids = [i.strip() for i in ids.split(",")]
    elif '.csv' in ids or '.txt' in ids:
        fb_ids = [i[0].strip() for i in load_data(ids)]
    else:
        fb_ids = [ids]

    try:
        end_dateobj = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        end_dateobj = ''

    if scrape_mode == 'feed' or scrape_mode == 'posts':
        header = ['from', 'from_id', 'message', 'picture', 'link', 'name', 'description', 'type', 'created_time',
                  'shares', 'likes', 'loves', 'wows', 'hahas', 'sads', 'angrys', 'post_id']
    else:
        header = ['from', 'from_id', 'comment', 'created_time', 'likes', 'post_id', 'original_poster',
                  'original_message']

    csv_data = []
    csv_data.insert(0, header)
    save_csv(outfile, csv_data, file_mode="a")

    for x, fid in enumerate(fb_ids):
        if scrape_mode == 'comments':
            msg_url = 'https://graph.facebook.com/v' + version + '/' + fid + '?fields=from,message&' + fb_token
            msg_json = url_retry(msg_url)
            if msg_json == False:
                print("URL not available. Continuing...", fid)
                continue
            msg_user = msg_json['from']['name']
            msg_content = optional_field(msg_json, 'message')
            field_list = 'from,message,created_time,like_count'
        else:
            msg_user = ''
            msg_content = ''
            field_list = 'from,message,picture,link,name,description,type,created_time,shares,likes.summary(total_count).limit(0)'

        data_url = 'https://graph.facebook.com/v' + version + '/' + fid.strip() + '/' + scrape_mode + '?fields=' + field_list + '&limit=100&' + fb_token

        # sys.exit()
        data_rxns = []
        new_rxns = ['LOVE', 'WOW', 'HAHA', 'SAD', 'ANGRY']
        for i in new_rxns:
            data_rxns.append(
                'https://graph.facebook.com/v' + version + '/' + fid.strip() + '/' + scrape_mode + '?fields=reactions.type(' + i + ').summary(total_count).limit(0)&limit=100&' + fb_token)

        next_item = url_retry(data_url)
        # with open("Output.txt", "w") as text_file:
        # print(next_item, file=text_file)



        if next_item != False:
            for n, i in enumerate(data_rxns):
                tmp_data = url_retry(i)
                for z, j in enumerate(next_item['data']):
                    try:
                        j[new_rxns[n]] = tmp_data['data'][z]['reactions']['summary']['total_count']
                    except (KeyError, IndexError):
                        j[new_rxns[n]] = 0

            csv_data = make_csv_chunk(next_item, scrape_mode, msg_user, msg_content)
            save_csv(outfile, csv_data, file_mode="a")
        else:
            print("Skipping ID " + fid + " ...")
            continue
        n = 0

        while 'paging' in next_item and 'next' in next_item['paging']:
            next_item = url_retry(next_item['paging']['next'])
            try:
                for i in new_rxns:
                    start = next_item['paging']['next'].find("from")
                    end = next_item['paging']['next'].find("&", start)
                    next_rxn_url = next_item['paging']['next'][
                                   :start] + 'reactions.type(' + i + ').summary(total_count).limit(0)' + \
                                   next_item['paging']['next'][end:]
                    tmp_data = url_retry(next_rxn_url)
                    for z, j in enumerate(next_item['data']):
                        try:
                            j[i] = tmp_data['data'][z]['reactions']['summary']['total_count']
                        except (KeyError, IndexError):
                            j[i] = 0
            except KeyError:
                continue

            csv_data = make_csv_chunk(next_item, scrape_mode, msg_user, msg_content)
            save_csv(outfile, csv_data, file_mode="a")
            try:
                print(n + 1, "page(s) of data archived for ID", fid, "at", next_item['data'][-1]['created_time'], ".",
                      round(time.time() - time1, 2), 'seconds elapsed.')
            except IndexError:
                break
            n += 1
            time.sleep(1)
            if end_dateobj != '' and end_dateobj > datetime.datetime.strptime(
                    next_item['data'][-1]['created_time'][:10], "%Y-%m-%d").date():
                break

        print(x + 1, 'Facebook ID(s) archived.', round(time.time() - time1, 2), 'seconds elapsed.')

    print('Script completed in', time.time() - time1, 'seconds.')
    return csv_data


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

def buildCommentsCSVs(client_id, client_secret, site_id, outfile_nodes, outfile_edges, version="2.10"):
    fb_token = getAccessToken(client_id, client_secret)
    since = '1506902400'
    until = '1508198400'
    field_list = 'id,message,created_time,comments{id,message,comments{id,message,from}}'
    #data_url = 'https://graph.facebook.com/v' + version + '/' + site_id + '?fields=posts{' + field_list + '}&limit=100&' + fb_token

    #&since='+since+'&until='+ until
    data_url = 'https://graph.facebook.com/v' + version + '/' + site_id + '/posts?fields=' + field_list + '&limit=100&since='+since+'&until='+until+'&' + fb_token
    next_item = url_retry(data_url)

    # set CSV headers
    headerNodeFile = ['node_id']
    csv_data = []
    csv_data.insert(0, headerNodeFile)
    save_csv(outfile_nodes, csv_data, file_mode="a")

    headerEdgeFile = ['source', 'target']
    csv_data = []
    csv_data.insert(0, headerEdgeFile)
    save_csv(outfile_edges, csv_data, file_mode="a")

    for post in next_item['data']:
        if 'comments' in post:
            addCommentsAndRepliesToCSV(post['comments'], outfile_nodes, outfile_edges )

    while 'paging' in next_item and 'next' in next_item['paging']:
        next_item = url_retry(next_item['paging']['next'])
        for post in next_item['data']:
            if 'comments' in post:
                addCommentsAndRepliesToCSV(post['comments'], outfile_nodes, outfile_edges)



def buildPostCSVs(client_id, client_secret, site_id, outfile_nodes, outfile_edges, version="2.10"):
    fb_token = getAccessToken(client_id, client_secret)
    since = '1506902400'
    until = '1508198400'
    field_list = 'id,comments{id,from}'
    #data_url = 'https://graph.facebook.com/v' + version + '/' + site_id + '?fields=posts{' + field_list + '}&limit=100&' + fb_token
    data_url = 'https://graph.facebook.com/v' + version + '/' + site_id + '/posts?fields=' + field_list + '&limit=100&since='+since+'&until='+until+'&' + fb_token
    next_item = url_retry(data_url)

    headerNodeFile = ['node_id']
    csv_data = []
    csv_data.insert(0, headerNodeFile)
    save_csv(outfile_nodes, csv_data, file_mode="a")

    headerEdgeFile = ['source', 'target']
    csv_data = []
    csv_data.insert(0, headerEdgeFile)
    save_csv(outfile_edges, csv_data, file_mode="a")

    for post in next_item['data']:
        addPostsAndCommentsToCSV(post,outfile_nodes,outfile_edges)

    while 'paging' in next_item and 'next' in next_item['paging']:
        next_item = url_retry(next_item['paging']['next'])
        for post in next_item['data']:
            addPostsAndCommentsToCSV(post, outfile_nodes, outfile_edges)






#115872105050?fields=posts{id,created_time,name}&limit=100
