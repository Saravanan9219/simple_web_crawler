import grequests
import threading
from Queue import Queue
from time import sleep
import lxml.html
import signal
import sys
import re
import os
import simplejson as json

html_dict = {} #Dict to keep record of urls
threads = []   #List of threads 


def coroutine(func):
    """Decorator function to decorate the html and url processing 
       coroutines to return corresponding queue and routine"""
    def new_func():
        routine = func()
        queue = routine.next()
        return queue, routine
    return new_func

def filter_links(link):
    """Filter links based on domain and filter out already crawled links"""
    if domain_pattern.match(str(link[2])) \
            and len(html_dict.keys()) <= MAX_URLS:         
        new_link = domain_pattern.match(link[2]).group(0)
        if new_link in html_dict.keys():
            return False
        html_dict[new_link] = '' #Insert link in dict to avoid 
                                 #duplicates of of urls
        return True

    return False


@coroutine
def html_parser():
    """Coroutine function to create threads for extracting and
       returning links from the html page crawled"""
    html_queue = Queue()
    url_queue, url_routine = yield html_queue #return html queue and recieve
                                              #url queue and routine for 
                                              #further processing
    def html_worker():
        """Worker function for extracting links from html pages in
           the html queue"""
        if html_queue.empty():
            return
        html = html_queue.get()
        document = lxml.html.document_fromstring(html)
        document.make_links_absolute(domain)
        links = filter(filter_links, document.iterlinks())
        url_queue.put([link[2] for link in links if link[0].tag == 'a' \
                          and link[2] != 'javascript:;'\
                          and len(html_dict.keys()) <= MAX_URLS])
        while True:
            try:
                url_routine.send('start thread')
                break;
            except Exception as e:
                print e
                sleep(3)
    while True:
        try:
            command = yield
            if command == 'start thread':
                #Start a new thread
                thread = threading.Thread(target=html_worker)
                threads.append(thread)
                thread.start()
            else:
                print 'Invalid Command'
        except Exception as e:
            print e

@coroutine    
def url_parser():
    """Coroutine function to create threads to get html pages"""
    url_queue = Queue()
    html_queue, html_routine = yield url_queue #Return url_queue and recieve
                                               #html queue and routine 
                                               #for further processing
    def url_worker():
        """Worker for getting html pages of urls from the url queue"""
        if url_queue.empty():
            return
        urls = url_queue.get()
        requests = (grequests.get(url) for url in urls)
        responses = grequests.map(requests)
        for response in responses:
            if response and len(html_dict.keys()) <= MAX_URLS:
                html_queue.put(response.content)
                if response.url in html_dict.keys():
                    print response.url, response.status_code
                    html_dict[response.url] = response.content

                    filename = response.url.replace('.', '_')\
                                   .replace('/', '_')\
                                   .replace(':', '_')+ '.html'

                    with open('html_files/' + filename, 'wb') as file:
                        file.write(response.content)

        while True:
            try:
                html_routine.send('start thread')
                break

            except Exception as e:
                print e
                sleep(3)
    while True:
        try:
            command = yield
            if command == 'start thread':
                #Start a new thread
                thread = threading.Thread(target = url_worker)
                threads.append(thread)
                thread.start()

            else:
                print 'Invalid Command'

        except Exception as e:
            print e

def handler(signum, frame):
    """Signal handler to exit script"""
    if signum == 2:
        print 'Exiting the Script'
        sys.exit()


if __name__ == '__main__':
    signal.signal(signal.SIGINT, handler)
    domain = raw_input('Enter the url: (eg: http://chennaipy.org)\n')
    MAX_URLS = raw_input('Enter the number of urls to be crawled (eg: 5)\n')
    MAX_URLS = int(MAX_URLS)
    domain_pattern = re.compile(r'^' + domain + '(?:/[^\.#]*)*(?:.html)?$')
    if not os.path.exists('html_files'):
        os.mkdir('html_files')
    url_q, url_routine = url_parser()
    html_q, html_routine = html_parser()
    url_routine.send([html_q, html_routine])
    html_routine.send([url_q, url_routine])
    html_dict[domain] = ''
    url_q.put([domain])
    url_routine.send('start thread')
    for thread in threads:
        thread.join()
    with open('html_files/links_list.json', 'wb') as file:
        file.write(json.dumps(html_dict.keys()))
