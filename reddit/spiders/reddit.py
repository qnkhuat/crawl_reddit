import scrapy
from urllib.parse import urlencode
import json,arrow
import requests,os

class Subreddit(object):
    def __init__(self,data):
        """ 
        data(json) : json data from a thread
        """
        self.data = data  
        self._get_meta_data()
        self.check_content()


    def _get_meta_data(self):
        thread = self.data[0]['data']['children'][0]['data']

        # thread infos
        self.url= thread['url']
        self.score = thread['score']
        self.title = thread['title']
        self.selftext = thread['selftext']
        self.fulltext = [self.title,self.selftext]


        # comments
        comments_obj = self.data[1]['data']['children']
        self.comments = self.get_comments(comments_obj)
        

    def get_comments(self,comments):
        """ Extract comments from reddit comments object"""
        all_comments = []
        for comment in comments:
            try :
                all_comments.append({
                    'comment':comment['data']['body'],
                    'score':comment['data']['score']
                })
            except: pass
        return all_comments
    
    def check_content(self):
        self.is_removed =  self.selftext == '[removed]' or self.title == '[removed]'
        
    
    def to_dict(self):
        return {
            'comments':self.comments,
            'score':self.score,
            'title':self.title,
            'selftext':self.selftext,
            'fulltext':self.fulltext,
            'url':self.url,
            'is_removed':self.is_removed,
        }

    

class Reddit(scrapy.Spider):
    name = "reddit"

    QUERY_URL = 'https://api.pushshift.io/reddit/submission/search' 
    DAYS = 365*3
    INTERVAL = 1
    SCORE_THRESHOLD = 100

    
    # get current time
    now = arrow.utcnow()


    SUBREDDIT = 'jokes'
    
    # setup saving dir
    saved_count = 0
    save_folder = './data'
    end_day = now.format('DD-MM-YY')
    start_day = arrow.get(now.timestamp - 86400*DAYS).format('DD-MM-YY')

    output_folder = os.path.join(save_folder,
        ('_').join([SUBREDDIT,start_day,end_day,'interval',str(INTERVAL),'st',str(SCORE_THRESHOLD)])
        )
    os.makedirs(output_folder,exist_ok=True)



    def start_requests(self):
        urls = []
        for day in range(0,self.DAYS,self.INTERVAL):
            payload = {
                'after':str(day)+'d',
                'before':str(day-self.INTERVAL) + 'd',
                'size':500,
                'sort_type':'score',
                'is_video':'false',
                #'score':'>'+args.st,
                # the score from pushshift sometimes a little bit not up to date with
                # not popular threads, we can lost a lot of good threads there
                # so we get almost all threads and send user Subreddit to find it
                # real score
                'score':'>0',
                'subreddit':self.SUBREDDIT,
                'sort':'desc',
                #'fields':"title,score,selftext,url,created_utc,num_comments",
                }

            payload_string = urlencode(payload)
            url = self.QUERY_URL + '?' + payload_string

            start_day = arrow.get(self.now.timestamp - 86400*(day-self.INTERVAL)).format('DD-MM-YY')
            end_day= arrow.get(self.now.timestamp - 86400*day).format('DD-MM-YY')

            # file name for save
            save_file = os.path.join(self.output_folder,('_').join([start_day,end_day]) + '.json')

            yield scrapy.Request(url=url, callback=self.parse,meta={'save_file':save_file})

    def parse(self, response):
        # json data from pushshift, contains many subreddit threads
        data_pushshift = json.loads(response.text)
        for thread in data_pushshift['data']:
            full_link = thread['full_link'] + '.json'
            yield scrapy.Request(full_link,self.extract_thread,meta={'save_file':response.meta['save_file']})

    def extract_thread(self,response):
        data = json.loads(response.text)
        save_file = response.meta['save_file']
        subreddit = Subreddit(data)
        
        thread = subreddit.to_dict()
        if subreddit.score > self.SCORE_THRESHOLD : 
            try : 
                if os.path.isfile(save_file):
                    # save_file already created
                    with open(save_file) as f :
                        saved_data = json.load(f)
                    saved_data.append(thread)

                    with open(save_file,'w') as f:
                        json.dump(saved_data,f)
                else:
                    # First thread of this day interval
                    with open(save_file,'w') as f:
                        json.dump([thread],f)
                self.saved_count+=1
                print(f'{self.saved_count} files saved')
            except Exception as e:
                print(f'Something bad happened!!! EXCEPTION : {e}')

                    
        


            
            
            
            
            
            
            
            
            
            










        
