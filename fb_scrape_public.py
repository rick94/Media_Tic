'''
This script can download posts and comments from public Facebook pages and groups. It requires Python 3.
INSTRUCTIONS
1.    This script is written for Python 3 and won't work with previous Python versions.
2.    The main function in this module is scrape_fb (see lines 107-109). It is the only function most users will need to run directly.
3.    You need to create your own Facebook app, which you can do here: https://developers.facebook.com/apps . Doesn't matter what you call your new app, you just need to pull its unique client ID (app ID) and app secret.
4.    Once you create your app, you can insert the client ID and secret AS STRINGS into the appropriate scrape_fb fields. 
5.    This function accepts text FB user IDs ('barackobama'), numerical user IDs, and post IDs. You can load them into the ids field using a comma-delimited string or by creating a plain text file in the same folder as the script containing one or more names of the Facebook pages you want to scrape, one ID per line (this file MUST have a .csv or .txt extension). For example, if you wanted to scrape Barack Obama's official FB page (http://facebook.com/barackobama/) using the text file method, your first line would simply be 'barackobama' without quotes. I suggest starting with only one ID to make sure it works. You'll only be able to collect data from public pages.
6.    The only required fields for the scrape_fb function are client_id, client_secret, and ids. I recommend not changing the other defaults (except for maybe outfile) unless you know what you're doing.
7.    If you did everything correctly, the command line should show you some informative status messages. Eventually it will save a CSV full of data to the same folder where this script was run. If something went wrong, you'll see an error.
'''

import sys
import copy
import csv
import datetime
import json
import socket
import time
import urllib.request
socket.setdefaulttimeout(30)

def load_data(data,enc='utf-8'):
    if type(data) is str:
        csv_data = []
        with open(data,'r',encoding = enc,errors = 'replace') as f:
            reader = csv.reader((line.replace('\0','') for line in f)) #remove NULL bytes
            for row in reader:
                if row != []:
                    csv_data.append(row)
        return csv_data
    else:
        return copy.deepcopy(data)

def save_csv(filename,data,use_quotes=True,file_mode='w',enc='utf-8'): #this assumes a list of lists wherein the second-level list items contain no commas
    with open(filename,file_mode,encoding = enc) as out:
        for line in data:
            if use_quotes == True:
                row = '"' + '","'.join([str(i).replace('"',"'") for i in line]) + '"' + "\n"
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

def optional_field(dict_item,dict_key):
    try:
        out = dict_item[dict_key]
        if dict_key == 'shares':
            out = dict_item[dict_key]['count']
        if dict_key == 'likes':
            out = dict_item[dict_key]['summary']['total_count']
    except KeyError:
        out = ''
    return out
    
def make_csv_chunk(fb_json_page,scrape_mode,thread_starter='',msg=''):
    csv_chunk = []
    if scrape_mode == 'feed' or scrape_mode == 'posts':
        for line in fb_json_page['data']:
            csv_line = [line['from']['name'], \
            '_' + line['from']['id'], \
            optional_field(line,'message'), \
            optional_field(line,'picture'), \
            optional_field(line,'link'), \
            optional_field(line,'name'), \
            optional_field(line,'description'), \
            optional_field(line,'type'), \
            line['created_time'], \
            optional_field(line,'shares'), \
            optional_field(line,'likes'), \
            optional_field(line,'LOVE'), \
            optional_field(line,'WOW'), \
            optional_field(line,'HAHA'), \
            optional_field(line,'SAD'), \
            optional_field(line,'ANGRY'), \
            line['id']]
            csv_chunk.append(csv_line)
    if scrape_mode == 'comments':
        for line in fb_json_page['data']:
            csv_line = [line['from']['name'], \
            '_' + line['from']['id'], \
            optional_field(line,'message'), \
            line['created_time'], \
            optional_field(line,'like_count'), \
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

def scrape_fb(client_id,client_secret,ids,outfile="fb_data.csv",version="2.7",scrape_mode="feed",end_date=""):

    time1 = time.time()
    if type(client_id) is int:
        client_id = str(client_id)
    fb_urlobj = urllib.request.urlopen('https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id=' + client_id + '&client_secret=' + client_secret)
    fb_token = 'access_token=' + json.loads(fb_urlobj.read().decode(encoding="latin1"))['access_token']
    if "," in ids:
        fb_ids = [i.strip() for i in ids.split(",")]
    elif '.csv' in ids or '.txt' in ids:
        fb_ids = [i[0].strip() for i in load_data(ids)]
    else:
        fb_ids = [ids]

    r = appendReactions(version,ids,fb_token)
    try:
        end_dateobj = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        end_dateobj = ''

    if scrape_mode == 'feed' or scrape_mode == 'posts':
        header = ['from','from_id','message','picture','link','name','description','type','created_time','shares','likes','loves','wows','hahas','sads','angrys','post_id']
    else:
        header = ['from','from_id','comment','created_time','likes','post_id','original_poster','original_message']

    csv_data = []
    csv_data.insert(0,header)
    save_csv(outfile,csv_data,file_mode="a")

    for x,fid in enumerate(fb_ids):
        if scrape_mode == 'comments':
            msg_url = 'https://graph.facebook.com/v' + version + '/' + fid + '?fields=from,message&' + fb_token
            msg_json = url_retry(msg_url)
            if msg_json == False:
                print("URL not available. Continuing...", fid)
                continue
            msg_user = msg_json['from']['name']
            msg_content = optional_field(msg_json,'message')
            field_list = 'from,message,created_time,like_count'
        else:
            msg_user = ''
            msg_content = ''
            field_list = 'from,message,picture,link,name,description,type,created_time,shares,likes.summary(total_count).limit(0)'
            
        data_url = 'https://graph.facebook.com/v' + version + '/' + fid.strip() + '/' + scrape_mode + '?fields=' + field_list + '&limit=100&' + fb_token

        #sys.exit()
        data_rxns = []
        new_rxns = ['LOVE','WOW','HAHA','SAD','ANGRY']
        for i in new_rxns:
            data_rxns.append('https://graph.facebook.com/v' + version + '/' + fid.strip() + '/' + scrape_mode + '?fields=reactions.type(' + i + ').summary(total_count).limit(0)&limit=100&' + fb_token)
        
        next_item = url_retry(data_url)
        #with open("Output.txt", "w") as text_file:
           # print(next_item, file=text_file)



        if next_item != False:
            for n,i in enumerate(data_rxns):
                tmp_data = url_retry(i)
                for z,j in enumerate(next_item['data']):
                    try:
                        j[new_rxns[n]] = tmp_data['data'][z]['reactions']['summary']['total_count']
                    except (KeyError,IndexError):
                        j[new_rxns[n]] = 0

            csv_data = make_csv_chunk(next_item,scrape_mode,msg_user,msg_content)
            save_csv(outfile,csv_data,file_mode="a")
        else:
            print("Skipping ID " + fid + " ...")
            continue
        n = 0

        for i in next_item['data']:
            buildNodeInsertQuery(i,"")


        while 'paging' in next_item and 'next' in next_item['paging']:
            next_item = url_retry(next_item['paging']['next'])
            try:
                for i in new_rxns:
                    start = next_item['paging']['next'].find("from")
                    end = next_item['paging']['next'].find("&",start)
                    next_rxn_url = next_item['paging']['next'][:start] + 'reactions.type(' + i + ').summary(total_count).limit(0)' + next_item['paging']['next'][end:]
                    tmp_data = url_retry(next_rxn_url)
                    for z,j in enumerate(next_item['data']):
                        try:
                            j[i] = tmp_data['data'][z]['reactions']['summary']['total_count']
                        except (KeyError,IndexError):
                            j[i] = 0
            except KeyError:
                continue
            
            csv_data = make_csv_chunk(next_item,scrape_mode,msg_user,msg_content)
            save_csv(outfile,csv_data,file_mode="a")
            try:
                print(n+1,"page(s) of data archived for ID",fid,"at",next_item['data'][-1]['created_time'],".",round(time.time()-time1,2),'seconds elapsed.')
            except IndexError:
                break
            n += 1
            time.sleep(1)
            if end_dateobj != '' and end_dateobj > datetime.datetime.strptime(next_item['data'][-1]['created_time'][:10],"%Y-%m-%d").date():
                break
            
        print(x+1,'Facebook ID(s) archived.',round(time.time()-time1,2),'seconds elapsed.')

    print('Script completed in',time.time()-time1,'seconds.')
    return csv_data
    
#A partir de ac� se crearan funciones propias, luego se tratar� de limpiar el c�digo tanto como sea posible
# para que solo queden funcionalidades que si se est�n necesitando

def getAccessToken(client_id, client_secret):
    fb_urlobj = urllib.request.urlopen('https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id=' + client_id + '&client_secret=' + client_secret)
    fb_token = 'access_token=' + json.loads(fb_urlobj.read().decode(encoding="latin1"))['access_token']
    return fb_token

def appendReactions(version, post_id, fb_token):
    data_url = 'https://graph.facebook.com/v' + version + '/' + post_id.strip() + '?fields=reactions&limit=100&' + fb_token
    json_data = url_retry(data_url)
    next_item = json_data['reactions']
    reactors_list = next_item['data']
    while 'paging' in next_item and 'next' in next_item['paging']:
        next_item = url_retry(next_item['paging']['next'])
        try:
            reactors_list.extend(next_item['data'])
        except KeyError:
            continue
    return reactors_list



def buildPostJson(version, scrape_mode, fid, fb_token):
    msg_user = ''
    msg_content = ''
    field_list = 'from,message,picture,link,name,description,type,created_time,shares,likes.summary(total_count).limit(0)'
    data_url = 'https://graph.facebook.com/v' + version + '/' + fid.strip() + '/' + scrape_mode + '?fields=' + field_list + '&limit=100&' + fb_token
    next_item = url_retry(data_url)




def buildReactionsQueries(version, scrape_mode, fid, fb_token):
    data_rxns = []
    new_rxns = ['LOVE','WOW','HAHA','SAD','ANGRY']
    for i in new_rxns:
        data_rxns.append('https://graph.facebook.com/v' + version + '/' + fid.strip() + '/' + scrape_mode + '?fields=reactions.type(' + i + ').summary(total_count).limit(0)&limit=100&' + fb_token)
    return data_rxns

def buildNodeInsertQuery(line, type):
    id = line['id']
    link =  optional_field(line,'link')
    title = optional_field(line,'name')
    typePost = optional_field(line,'type')
    postDate =  line['created_time']
    share_count = optional_field(line,'shares')
    if share_count == '': share_count = 0
    like_count =  optional_field(line, 'likes')
    loves = optional_field(line,'LOVE' )
    wows   = optional_field(line,'WOW' )
    hahas = optional_field(line,'HAHA' )
    sads = optional_field(line,'SAD')
    angries = optional_field(line,'ANGRY' )
    engagement = like_count + loves + wows + hahas + sads + angries
    queryList = ["CREATE (n:Post { id: '",id, "', link: '",link,"', title: '", title, "', type: '", typePost,\
                 "', postDate: '", postDate, "', share_count: ", str(share_count) , ", like_count: ",str(like_count),\
                 ", loves: ", str(loves),", wows: ",str(wows), ", hahas: ", str(hahas), ",sads: ", str(sads), ", angries: ", str(angries),\
                 ", engagement: ", str(engagement),"})"]
    queryString = "".join(queryList)
    print(queryString)
    return queryString


def scrape_fb1(client_id, client_secret):
    accessToken = getAccessToken(client_id, client_secret)