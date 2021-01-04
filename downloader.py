from bs4 import BeautifulSoup
from datetime import datetime
import getpass
from urllib.parse import urljoin
import os
import requests
import threading
import zipfile
from tqdm import tqdm

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
    FILE_BLOCK = 4096 * 1024
    def __init__(self, num_threads=4, chunk_size=8192, session=None):
        self.session = session
        assert num_threads > 0
        self.num_threads = num_threads
        self.thread_pool = [0] * num_threads
        assert chunk_size > 0
        self.chunk_size = chunk_size
        self._continue = False


    def Handler(self, options):
        if os.path.exists(options['file_path']):
            os.remove(options['file_path'])
        thread_id = options['thread_id']
        headers = {'Range': 'bytes={0}-{1}'.format(options['start'], options['end'])}
        resp = self.session.get(options['url'], headers=headers, stream=True)
        # Check wheather server supports accept ranges
        # if not close extra threads with an empty file early
        if  'Content-Range' not in resp.headers and thread_id != 0:
             resp.close()
             options['progress'][thread_id] = 1.0
             options['status'][thread_id] = 'canceled - range not supported'
             return # kill the thread

        options['progress'][thread_id] = 0.0
        total = resp.headers['Content-Length']
        step = float(self.chunk_size) / float(total)
        pbar = tqdm(unit=" Bytes", total=int(total), position=thread_id,
                    desc ="Thread ", colour='blue', leave=True)
        with open(options['file_path'], "wb") as fp:
            for chunk in resp.iter_content(chunk_size=self.chunk_size):
                if not self._continue:
                    break
                fp.write(chunk)
                ln = len(chunk)
                pbar.update (ln)
                options['progress'][thread_id] += step
        options['progress'][thread_id] = 1.0
        options['status'][thread_id] = 'succeed'
        resp.close()
        pbar.close()

    def cancel(self):
        self._continue = False

    def download(self, url, file_path, establish_session=False):
        if establish_session:
            self.session = requests.session()
        resp = self.session.head(url)
        file_size = int(resp.headers['Content-Length'])
        accept_range = 'Accept-Ranges' in resp.headers.keys()
        if accept_range:
            assert resp.headers['Accept-Ranges'] == 'bytes'
            num_threads = self.num_threads
        else:
            num_threads = 1
        thread_part = int(file_size / self.num_threads)
        progress_percents = [0] * num_threads
        thread_status = ['frozen'] * num_threads
        # Reserve the final file name
        with open(file_path, 'wb') as fid:
            pass
        print("Downloading {0} in progress".format(url))
        self._continue = True
        for counter in range(num_threads):
            start = counter * thread_part
            if counter == num_threads - 1:
                end = file_size
            else:
                end = start + thread_part - 1
            thread = threading.Thread(target=self.Handler, kwargs={'options':
                    {'start':start, 'end':end, 'url':url,
                     'file_path':file_path + ".part{0}".format(counter),
                     'accept_range':accept_range,
                     'progress':progress_percents, 'thread_id':counter,
                     'thread_ID':counter, 'status':thread_status}})
            self.thread_pool[counter] = thread
            thread.setDaemon(True)
            thread.start()
        # Progress report and wait part
        active_threads = [t for t in self.thread_pool[:num_threads] if t.is_alive()]
        for thread in self.thread_pool[:num_threads]:
           thread.join()
        os.remove(file_path)
        os.rename(file_path + ".part0", file_path)
        assembly_parts = [counter for counter in range(1, num_threads) if thread_status[counter]=='succeed']
        if any(assembly_parts):
            with open(file_path, 'ab') as out_file:
                for counter in assembly_parts:
                    with open(file_path + ".part{0}".format(counter), 'rb') as in_file:
                        out_file.write(in_file.read(DownloadManager.FILE_BLOCK))
        return 1 # return a success code

class Decompressor(object):
    def decompress_zip(inpfile, outdir):
        with zipfile.ZipFile(inpfile, 'r') as zip_fid:
            zip_fid.extractall(outdor)

    def decompress(inpfile, outdir):
        _, ext = os.path.splitext(inpfile)
        if ext == "zip":
            return Decompressor.decompress_zip(inpfile, outdir)

class NinaProDownloader(object):
    NINA_URL = "http://ninapro.hevs.ch"
    NINA_LOGIN_FORM = "user-login-form"
    NINA_MAIN_MENU = "main-menu-links"
    def __init__(self, user_name=None, password=None, session=None):
        try:
            if session is None:
                self.session = None
                session = requests.session()
                print("Establishing a session....")
                resp = session.get(NinaProDownloader.NINA_URL)
                soup = BeautifulSoup(resp.text, 'html.parser')
                form = soup.find_all("form")
                # if there are more than a form look for the login form
                form = [f for f in form if f.attrs.get("id") == NinaProDownloader.NINA_LOGIN_FORM]
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
                url = urljoin(NinaProDownloader.NINA_URL, form.attrs.get("action"))
                if form.attrs.get("method").lower() == "post":
                    resp = session.post(url, data=data)
                else:
                    resp = session.get(url, params=data)
                if resp.status_code == 200:
                    print("---- Successfully logged in.")
                    self.session = session
            else:
                self.session = session
            # get the main page
            resp = session.get(NinaProDownloader.NINA_URL)
            assert resp.status_code == 200
            soup = BeautifulSoup(resp.text, 'html.parser')
            ul = soup.find_all("ul")
            ul = [u for u in ul if u.attrs.get("id") == NinaProDownloader.NINA_MAIN_MENU]
            assert len(ul) == 1
            ul = ul[0]
            lis = ul.find_all("li")
            self.data_sets = {li.text:urljoin(NinaProDownloader.NINA_URL, li.find("a").attrs.get("href"))
                                                for li in lis if li.text.startswith("DB")}
        except:
            print("---- Error logging in Ninapro!")



    def download_db(self, db_num, out_dir):
        def _parse_info_table(resp_text, delimiter=','):
            soup = BeautifulSoup(resp_text, 'html.parser')
            table = soup.find_all("table")
            assert len(table) == 1
            table = table[0]
            # find the table headers
            headers = [th.text.replace("\n", "").strip()  for th in table.find_all("th")]
            # get table rows (recycle the table var)
            table = table.find_all("tr")
            # skip the first row (headers)
            table = table[1:]
            download_list = []
            csv_tab = [delimiter.join(headers)]
            for tr in table:
                row = [td.text.replace("\n", "").strip() for td in  tr.find_all("td")]
                csv_tab.append(delimiter.join(row))
                links = [(a.text, a.attrs.get("href")) for a in tr.find_all("a") if "." in a.text]
                links = [(name, link if link.startswith("http")
                        else urljoin(NinaProDownloader.NINA_URL, link)) for name, link in links]
                download_list += links
            return "\n".join(csv_tab), download_list

        assert 0 < db_num < 10
        assert db_num == int(db_num)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            print("The output directory is created.")
        db_url = urljoin(NinaProDownloader.NINA_URL, 'data{0}'.format(db_num))
        resp = self.session.get(db_url)
        csv_info, download_list = _parse_info_table(resp.text)
        # save the download page
        save_path = os.path.join(out_dir, "download_page.html")
        with open(save_path, 'wt') as fid:
            fid.write(resp.text)
        # Save csv file
        save_path = os.path.join(out_dir, "subject_info.csv")
        with open(save_path, "wt") as fid:
            fid.write(csv_info)
        now = lambda: str(datetime.now())

        with open (os.path.join(out_dir, "scraping.log"), 'wt') as log_fid:
            def log(x, skip_stamp=False):
                if skip_stamp:
                    log_line = x
                else:
                    log_line = "[{0}] {1}".format(now(), x)
                print(log_line)
                log_fid.write(log_line + "\n")

            log("Quality of Life Technology - UTD\nEMG Analysis Project\nDatabase scarper\n", True)
            log("database info: {0}\n".format(self.data_sets['DB{0}'.format(db_num)]), True)
            log("start scraping {0} files".format(len(download_list)))
            dm = DownloadManager(session=self.session, num_threads=4)
            log("Initiating download manager with 4 threads")
            for name, url in download_list:
                log("    downloading {0} started".format(url))
                save_path = os.path.join(out_dir, name)
                ret = dm.download(url, save_path)
                if ret:
                    log("        file saved to {0}".format(save_path))
                else:
                    log("        Error in downloading")
            log("Downloading database is complete! {0} files are downloaded".format(len(download_list)))
            return 1
