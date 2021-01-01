
from bs4 import BeautifulSoup
import getpass
from urllib.parse import urljoin
import requests
import threading
import pdb

__author__ = 'Mehrdad Heydarzadeh'
__copyright__ = 'Copyright 2020, EMG Analysis'
__credits__ = ['Quality of Life Lab', 'University of Texas at Dallas']
__license__ = 'MIT'
__version__ = '1.0.0'
__maintainer__ = 'Mehrdad Heydarzadeh'
__email__ = 'mehhey@gmail.com'
__status__ = 'work in progress'

class DownloadManager(object):
    def __init__(self, num_threads=4, chunk_size=8192, session=None):
        self.session = session
        self.num_threads = num_threads
        self.chunk_size = chunk_size
        self._continue = False
    
    def Handler(start, end, url, filename): 
        # specify the starting and ending of the file 
        headers = {'Range': 'bytes={0}-{1}'.format(start, end)} 
        # request the specified part and get into variable     
        r = requests.get(url, headers=headers, stream=True) 
        # open the file and write the content of the html page  
        # into file. 
        with open(filename, "r+b") as fp: 
            fp.seek(start) 
            var = fp.tell() 
            fp.write(r.content) 
    
    def download(url, file_path):


class NinaProDownloader(object):
    NINA_URL = "http://ninapro.hevs.ch"
    NINA_LOGIN_FORM = "user-login-form"
    def __init__(self, user_name=None, password=None):
        self.session = None
        session = requests.session()
        print("Establishing a session....")
        try:
            resp = session.get(NinawebDownloader.NINA_URL)
            soup = BeautifulSoup(resp.text, 'html.parser')
            form = soup.find_all("form")
            # if there are more than a form look for the login form
            form = [f for f in form if f.attrs.get("id") == NinawebDownloader.NINA_LOGIN_FORM]
            assert len(form) == 1
            form = form[0]
            # prepare the form for submission
            data = dict()
            for inp in form.find_all("input"):
                typ =  inp.attrs.get("type").lower()
                if typ == "submit":
                    continue
                if typ == "hidden":
                    data[inp.attrs.get("name")] = inp.attrs.get("value")
                if typ == "password":
                    pass_var = inp.attrs.get("name")
                if typ == "text":
                    usr_var = inp.attrs.get("name")
            # Add user name and pass to form
            if user_name is None:
                print("user name: ")
                user_name = input()
            data[usr_var] = user_name

            if password is None:
                password = getpass.getpass("password: ")
            data[pass_var] = password

            # submit the form
            url = urljoin(NinawebDownloader.NINA_URL, form.attrs.get("action"))
            if form.attrs.get("method").lower() == "post":
                resp = session.post(url, data=data)
            else: 
                resp = session.get(url, params=data)
            if resp.status_code == 200:
                print("---- Successfully logged in.")
                self.session = session
        except:
            print("---- Error logging in Ninapro!")
