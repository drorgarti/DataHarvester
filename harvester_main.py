import time
import random
import http
import urllib
import requests
import csv
import codecs
import sys, traceback
import os.path
from bs4 import BeautifulSoup
import uuid
import urllib.request
import datetime
from time import sleep
import os.path


class DataHarvester(object):

    DRIVE_LETTER = 'C'
    ANGELLIST_DIR = 'temp\AcureRate\DATA\AngelList Data\AngelList Scraping'
    COMPANY_ANCHOR_FOLDER = r'%s:\%s\Companies Anchors' % (DRIVE_LETTER, ANGELLIST_DIR)
    COMPANY_DETAILS_FOLDER = r'%s:\%s\Companies' % (DRIVE_LETTER, ANGELLIST_DIR)
    COMPANIES_LOGOS_FOLDER = r'%s:\%s\Companies\Logos' % (DRIVE_LETTER, ANGELLIST_DIR)
    PEOPLE_DETAILS_FOLDER = r'%s:\%s\People' % (DRIVE_LETTER, ANGELLIST_DIR)
    PEOPLE_PHOTOS_FOLDER = r'%s:\%s\People\Photos' % (DRIVE_LETTER, ANGELLIST_DIR)

    DOWNLOAD_PHOTOS = True
    UPDATE_COUNT = 100  # if -1, the scraping will never stop... :-)

    def __init__(self):

        self.angellist_company_properties = ['name', 'angellist url', 'display name', 'description', 'location', 'market tags',
                                'company size', 'photo url', 'photo uuid', 'company url', 'twitter url', 'facebook url', 'linkedin url',
                                'producthunt url', 'blog url', 'founders', 'people', 'companies', 'portfolio companies', 'row count']

        self.angellist_person_properties = ['name',
                                            'angellist url',
                                            'redirecting_company',
                                            'redirecting_url',
                                            'display name',
                                            'description',
                                            'location',
                                            'photo url',
                                            'photo uuid',
                                            'jobs',
                                            'num_connections',
                                            'linkedin', 'twitter', 'facebook', 'rss', 'website',
                                            'limited profile',
                                            'score']

        self.proxy_url = 'http://lum-customer-acurerate-zone-residential:1b05274a7daf@zproxy.luminati.io:22225'
        self.user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/60.0.3112.101 Safari/537.36'
        self.headers = {
            # 'contentType': 'application/json; charset=utf-8',
            'User-Agent': self.user_agent,
            'X-Csrf-Token': 'qHd20i05cSh_RVi6i_nqqHnG5fMTJPSrCeado5KWV8s',
            'X-Requested-With': 'XMLHttpRequest'
        }

        self.num_consecutive_failures = 0
        pass

    @staticmethod
    def generate_uuid():
        uuid_object = uuid.uuid4()
        return str(uuid_object)

    @staticmethod
    def get_now_as_str():
        from time import gmtime, strftime
        str = strftime("%Y-%m-%d %H:%M:%S")
        return str

    def perform_request(self, url):

        # Check url validity
        if not url.startswith('http://') and not url.startswith('https://'):
            return 901, 'not a valid URL'

        retries = 0
        while retries < 3:
            try:
                res = requests.get(url, headers=self.headers)
                rc = res.status_code
                txt = res.content.decode("utf-8")
                break
            except requests.exceptions.ConnectionError as e:
                txt = "Connection refused - %s" % url
                print('Exception %s raised. retries: %s' % (e, retries))
                #sleep(300)  # Sleep 1 minute
            except Exception as e:
                txt = '<%s>' % e
                print('Exception %s raised. retries: %s' % (e, retries))
                #sleep(300)  # Sleep 1 minute
            retries += 1

        if retries == 3:
            rc = 901

        return rc, txt

    @staticmethod
    def perform_request_via_proxy(url, opener, data=None, with_ip=True, should_delay=True):

        if should_delay:
            delay = random.uniform(0.05, 0.15)
            print('Going to sleep: %s secs' % delay)
            time.sleep(delay)

        ip = None

        try:
            if data:
                response = opener.open(url, data)
            else:
                response = opener.open(url)
            content = response.read()
            txt = content.decode("utf-8")
            rc = response.status
            if with_ip:
                for (k, v) in response.headers._headers:
                    if k == 'X-Process':
                        ip = v
                        break
        except http.client.IncompleteRead as e:
            txt = '<%s>' % e
            rc = 902
        except urllib.error.HTTPError as e:
            txt = '<%s>' % e
            rc = e.code
        except urllib.error.URLError as e:
            txt = '<%s>' % e
            rc = 901
        except OSError as e:
            txt = '<%s>' % e
            rc = 900
        except Exception as e:
            txt = '<%s>' % e
            rc = 901

        return rc, txt, ip

    @staticmethod
    def rip_company_anchors_from_index_pages():

        from bs4 import BeautifulSoup
        from string import ascii_lowercase

        import urllib.request
        import sys

        #cj = CookieJar()
        #opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))

        #proxy_url = 'http://127.0.0.1:24001'
        #proxy_url = 'http://127.0.0.1:22225'
        #proxy_url = 'http://lum-customer-hl_7303b046-zone-static:difwir8myhu1@zproxy.luminati.io:22225'
        #proxy_url = 'http://lum-customer-acurerate-zone-static:difwir8myhu1@zproxy.luminati.io:22225'
        proxy_url = 'http://lum-customer-acurerate-zone-residential:1b05274a7daf@zproxy.luminati.io:22225'

        # See list of agenst here:
        # --> http://www.useragentstring.com/pages/useragentstring.php
        user_agent1 = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/60.0.3112.101 Safari/537.36'
        user_agents = [
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/60.0.3112.101 Safari/537.36',
            'Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48B; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/43.0.2357.65 Mobile Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
        ]

        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({'https': proxy_url})
        )
        opener.addheaders = [('User-Agent', user_agent1)]

        # Read the company names:
        # Example: https://angel.co/directory/companies/f-51'
        base_url = r'https://angel.co/directory/companies/%s-%s-%s'
        headers = requests.utils.default_headers()
        headers.update({'User-Agent': 'Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48B; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/43.0.2357.65 Mobile Safari/537.36'})

        output_file_name = ''
        go_go_go = True

        problem = 0
        user_agent_index = 0
        rotate_user_agent_count = 0
        requests_count = 0

        captch_bypass_attempts = 3
        delay_after_captch = 600  # 10 minutes
        main_page_range = range(1, 125)
        sub_page_range = range(1, 125)
        custom_main_page_start = 1
        custom_sub_page_start = 11
        #letters_range = ['p', 'f', 'z', 'b', 'c', 'a', 'd']
        letters_range = ['n', 'z']
        #letters_range = ascii_lowercase

        # Read last line of file
        # Todo...
        # companies_file = open(output_file_name, 'r', encoding="utf-8")
        # last_line = AcureRateUtils.tail(companies_file, 1, offset=None)

        rc = 0
        for c in letters_range:
            # Open file
            companies_file = open(r'%s\%s_angellist_companies.csv' % (DataHarvester.COMPANY_ANCHOR_FOLDER, c.upper()), 'a', encoding="utf-8")
            for i in main_page_range:
                if custom_main_page_start and i < custom_main_page_start:
                    continue
                custom_main_page_start = None
                if rc == 404:
                    break
                for j in sub_page_range:
                    if custom_sub_page_start and j < custom_sub_page_start:
                        continue
                    custom_sub_page_start = None
                    if rc == 404:
                        if j > 1:
                            rc = 0
                        else:
                            pass
                        break
                    url = base_url % (c, i, j)
                    # Issue out the request until we succeed or give-up:
                    local_delay = delay_after_captch
                    for attempt in range(1, captch_bypass_attempts):
                        ip = 'unknown'
                        start = time.time()
                        rc, txt, ip = DataHarvester.perform_request_via_proxy(url, opener, with_ip=True, should_delay=False)
                        end = time.time()
                        delta = end-start
                        print('[req time: %s]' % delta)

                        if rc >= 900:
                            print('Something really fishy is happening... Retrying.')
                            continue

                        requests_count += 1

                        if rc == 302 or txt.find('some unusual activity') != -1:
                            random_session_id = int(random.uniform(60000, 99999))
                            proxy_url = 'http://lum-customer-acurerate-zone-residential-session-rand%s:1b05274a7daf@zproxy.luminati.io:22225' % random_session_id
                            opener = urllib.request.build_opener(
                                urllib.request.ProxyHandler({'https': proxy_url})
                            )
                            opener.addheaders = [('User-Agent', user_agent1)]
                            requests_count = 0
                            now_str = DataHarvester.get_now_as_str()
                            print('%s: Got CAPTCHA! Ran %s requests. Reseting the session :)' % (now_str, requests_count))
                            continue

                        # Check if we hit a CAPTCHA
                        if rc == 302 or txt.find('some unusual activity') != -1:
                            now_str = DataHarvester.get_now_as_str()
                            print('%s: Got CAPTCHA! Ran %s requests. Going to sleep for %s seconds... Attempt #%s' % (now_str, requests_count, local_delay, attempt))
                            companies_file.write('Unable to read url %s (got CAPTCHA). Attempt #%s\n' % (url, attempt))
                            requests_count = 0
                            time.sleep(local_delay)
                            local_delay = local_delay * 2
                            continue
                        else:
                            break

                    # Check if we hit a problem:
                    if rc != 200:
                        print("Error %s: %s" % (rc, txt))
                        continue

                    # Rotate the user-agent
                    rotate_user_agent_count = (rotate_user_agent_count + 1) % 3
                    if rotate_user_agent_count == 0:
                        user_agent_index = (user_agent_index + 1) % len(user_agents)
                        # Set the new user-agent
                        opener.addheaders = [('User-Agent', user_agents[user_agent_index])]

                    # Parse page and write names
                    soup = BeautifulSoup(txt, 'html.parser')

                    now_str = DataHarvester.get_now_as_str()
                    print('%s: Processing now (req %s): %s-%s-%s' % (now_str, requests_count, c, i, j))
                    # Get company names
                    try:
                        elems = soup.findAll("div", {"class": "s-grid-colSm12"})
                        if elems:
                            for elem in elems:
                                name = elem.text.strip()
                                link = elem.find("a").get('href', None)
                                if ';' in name:
                                    name = "'" + name + "'"
                                # Write to file:
                                line = '%s;%s;%s;%s\n' % (ip, name, link, url)
                                line_fixed = ''.join([i if ord(i) < 128 else ' ' for i in line])
                                companies_file.write(line_fixed)
                    except Exception as e:
                        print('Exception raised while parsing elements in page: %s' % e)
                        companies_file.write('Exception raised while parsing elements in page: %s\n' % url)

                    # Flush all data collected so far
                    companies_file.flush()
            companies_file.close()

        if rc != 200:
            print('=====\nBROKE at %s (rc=%s)\n=====\n' % (url, rc))

        pass

    def _tail(self, file_path: str, n, offset=0, the_encoding='utf-8'):

        f = open(file_path, 'r', encoding=the_encoding)

        """Reads a n lines from f with an offset of offset lines."""
        avg_line_length = 74
        to_read = n + offset
        while 1:
            try:
                f.seek(-(avg_line_length * to_read), 2)
            except IOError:
                # woops.  apparently file is smaller than what we want
                # to step back, go to the beginning instead
                f.seek(0)
            pos = f.tell()
            #lines = f.read().splitlines()
            txt = f.read()
            lines = txt.splitlines()
            if len(lines) >= to_read or pos == 0:
                f.close()
                return lines[-to_read:offset and -offset or None]
            avg_line_length *= 1.3

    def slugify(self, filename):
        """
        Normalizes string, converts to lowercase, removes non-alpha characters,
        and converts spaces to hyphens.
        """
        import string
        valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
        #filename = "This Is a (valid) - filename%$&$ .txt"
        valid_filename = ''.join(c for c in filename if c in valid_chars)
        return valid_filename

    def quotify(self, text):
        if "|" in text or ";" in text:
            text = "'%s'" % text
        return text

    def download_photo(self, path, sub_folder, company_name, url):

        # Save it to the folder
        unique_str = DataHarvester.generate_uuid()
        file_name = '%s.jpg' % unique_str

        full_path = '%s\%s' % (path, sub_folder)

        relative_path_filename = '%s\%s' % (sub_folder, file_name)
        full_path_filename = '%s\%s' % (path, relative_path_filename)

        if not os.path.exists(full_path):
            os.makedirs(full_path)

        # Download photo from URL
        try:
            filename, headers = urllib.request.urlretrieve(url, full_path_filename)
        except Exception as e:
            relative_path_filename = None
            print('Failed to download photo for company %s (%s)' % (company_name, e))

        return relative_path_filename

    def calc_company_reliability_score(self, company_data):
        score = 0

        # Convert CSV row to dict
        data_obj = {}
        for x in range(0, len(self.angellist_company_properties)):
            key = self.angellist_company_properties[x]
            data_obj[key] = company_data[x].strip()

        # Check company's people (25%)
        persons = []
        if len(data_obj['founders']) > 0:
            persons.extend(data_obj['founders'].split(';'))
        if len(data_obj['people']) > 0:
            persons.extend(data_obj['people'].split(';'))
        persons_set = set(persons)
        num_people = len(persons_set)
        if num_people > 2:
            score += 25
        elif num_people == 2:
            score += 10
        elif num_people == 1:
            score += 5

        # Check if company has logo (25%)
        if len(data_obj['photo url']) > 2:
            score += 25

        # Check social links (25%)
        if len(data_obj['linkedin url']) > 2 and 'company' in data_obj['linkedin url']:
            score += 10
        if len(data_obj['company url']) > 2:
            score += 10
        if len(data_obj['twitter url'])+len(data_obj['facebook url'])+len(data_obj['blog url']) > 2:
            score += 5

        # Check meta data - description, tags, company size (25%)
        if len(data_obj['description']) > 5:
            score += 10
        if len(data_obj['market tags'].split(';')) > 1:
            score += 10
        if len(data_obj['company size']) > 0:
            score += 5

        return score

    def parse_person_profile_request_result(self, letter, txt, company_name, company_url, person_name, angellist_url, reliability_score):
        data_obj = {
            'name': person_name,
            'angellist url': angellist_url,
            'redirecting_company': company_name,
            'redirecting_url': company_url,
            'score': reliability_score
        }

        soup = BeautifulSoup(txt, 'html.parser')

        # Check if it's a limited profile
        try:
            elems = soup.select("div.limited_profile")
            if elems and len(elems) == 1:
                data_obj['limited profile'] = 'true'
                # Find name
                e = elems[0].select("h1")
                if e and len(e) > 0:
                    data_obj['display name'] = e[0].text.strip()

                # Find description
                e = elems[0].select('div[data-object_type="Profile"]')
                if e and len(e) > 0:
                    data_obj['description'] = e[0].text.strip()

                # Find photo
                e = elems[0].select('img')
                if e and len(e) > 0:
                    data_obj['photo url'] = e[0]['src']
                    if DataHarvester.DOWNLOAD_PHOTOS:
                        relative_path = self.download_photo(r'%s' % DataHarvester.PEOPLE_PHOTOS_FOLDER, letter, person_name, e[0]['src'])
                        if relative_path:
                            data_obj['photo uuid'] = relative_path
                return data_obj
        except:
            pass

        # Get name
        try:
            elems = soup.select("div.profile-text h1")
            if elems and len(elems) == 1:
                data_obj['display name'] = elems[0].text.strip()
        except:
            print('Unable to locate person name attribute for %s' % person_name)

        # Get description
        try:
            elems = soup.select("div.profile-text h2 p")
            if elems and len(elems) == 1:
                data_obj['description'] = elems[0].text.strip()
        except:
            print('Unable to locate description attribute for %s' % person_name)

        # Get photo
        try:
            elems = soup.select("div.subheader-container div.photo img")
            if elems and 'nopic' in elems[0]['src']:
                print('no image for person %s' % person_name)

            if elems and len(elems) == 1 and 'nopic' not in elems[0]['src']:
                data_obj['photo url'] = elems[0]['src']
                if DataHarvester.DOWNLOAD_PHOTOS:
                    relative_path = self.download_photo(r'%s' % DataHarvester.PEOPLE_PHOTOS_FOLDER, letter, person_name, elems[0]['src'])
                    if relative_path:
                        data_obj['photo uuid'] = relative_path
        except:
            print('Unable to locate photo attribute for %s' % person_name)

        # Get Number connections
        try:
            elems = soup.select("div.connection-counts")
            if elems and len(elems) == 1:
                data_obj['num_connections'] = elems[0].text.strip()
        except:
            print('Unable to locate description attribute for %s' % person_name)

        # Location, Tags, Education
        try:
            elems = soup.select("div.profile-text div.subheader-tags div.tags")
            if elems and len(elems) == 1:
                data_obj['location'] = elems[0].select("span.fontello-location + a")[0].text
                # data_obj['tag'] = elems[0].select("span.fontello-tag-1 + a")[0].text
                # data_obj['college'] = elems[0].select("span.fontello-college + a")[0].text
        except:
            print('Unable to locate location for %s' % person_name)

        # LinkedIn
        try:
            elems = soup.select("div.profile-text div.profiles-show a.fontello-linkedin")
            if elems and len(elems) == 1:
                data_obj['linkedin'] = elems[0].attrs['href']
        except:
            print('Unable to locate linkedin url for %s' % person_name)

        # Facebook
        try:
            elems = soup.select("div.profile-text div.profiles-show a.fontello-facebook")
            if elems and len(elems) == 1:
                data_obj['facebook'] = elems[0].attrs['href']
        except:
            print('Unable to locate facebook url for %s' % person_name)

        # RSS
        try:
            elems = soup.select("div.profile-text div.profiles-show a.fontello-rss")
            if elems and len(elems) == 1:
                data_obj['rss'] = elems[0].attrs['href']
        except:
            print('Unable to locate RSS url for %s' % person_name)

        # Twitter
        try:
            elems = soup.select("div.profile-text div.profiles-show a.fontello-twitter")
            if elems and len(elems) == 1:
                data_obj['twitter'] = elems[0].attrs['href']
        except:
            print('Unable to locate twitter url for %s' % person_name)

        # Other websites
        try:
            elems = soup.select('div.profile-text div.profiles-show a[data-field="online_bio_url"]')
            if elems and len(elems) == 1:
                link_href = elems[0].attrs['href']
                link_name = elems[0].text
                data_obj['website'] = '%s | %s' % (link_name, link_href)
            if elems and len(elems) > 1:
                print('More than 1 websites... CHECK')

        except:
            print('Unable to locate twitter tag for %s' % person_name)

        # Get jobs
        try:
            jobs = []
            elems = soup.select("div.experience_container div.experience")
            for exp_elem in elems:
                j = {'company_logo': '', 'company_name': '', 'company_url': '', 'title': '', 'years': ''}
                e = exp_elem.select("div.photo img")
                if e and len(e) > 0:
                    j['company_logo'] = self.quotify(e[0]['src'].strip())
                e = exp_elem.select("div.text a")
                if e and len(e) > 0:
                    j['company_name'] = self.quotify(e[0].text.strip())
                    j['company_url'] = self.quotify(e[0]['href'])
                e = exp_elem.select("div.text span")
                if e and len(e) > 0:
                    j['title'] = self.quotify(e[0].text.strip())
                e = exp_elem.select("div.text span.line")
                if e and len(e) > 0:
                    j['years'] = self.quotify(e[0].text.strip())
                jobs.append(j)
            data_obj['jobs'] = '; '.join([f['company_name'] + ' | ' + f['title'] + ' | ' + f['years'] + ' | ' + f['company_url'] for f in jobs])
        except:
            print('Unable to locate person experiences attribute for %s' % person_name)

        return data_obj

    def parse_company_profile_request_result(self, letter, txt, company_name, angellist_url):

        data_obj = {
            'name': company_name,
            'angellist url': angellist_url,
        }

        soup = BeautifulSoup(txt, 'html.parser')

        # Get name
        try:
            elems = soup.select("div.summary h1")
            if elems and len(elems) == 1:
                data_obj['display name'] = elems[0].text.strip()
        except:
            print('Unable to locate name attribute for %s' % company_name)

        # Get description
        try:
            elems = soup.select("div.summary h2")
            if elems and len(elems) == 1:
                data_obj['description'] = elems[0].text.strip()
        except:
            print('Unable to locate description attribute for %s' % company_name)

        # Get photo
        try:
            elems = soup.select("div.summary div.photo img")
            if elems and len(elems) == 1 and 'nopic_' not in elems[0]['src']:
                data_obj['photo url'] = elems[0]['src']
                relative_path = self.download_photo(r'%s' % DataHarvester.COMPANIES_LOGOS_FOLDER, letter, company_name, elems[0]['src'])
                if relative_path:
                    data_obj['photo uuid'] = relative_path
        except:
            print('Unable to locate photo attribute for %s' % company_name)


        # Get location
        try:
            elems = soup.select("span.js-location_tags a.tag")
            if elems and len(elems) == 1:
                data_obj['location'] = elems[0].text.strip()
        except:
            print('Unable to locate photo attribute for %s' % company_name)

       # Get market tags
        try:
            tags = []
            elems = soup.select("span.js-market_tags a.tag")
            for elem in elems:
                tags.append(elem.text.strip())
            data_obj['market tags'] = ';'.join(tags)
        except:
            print('Unable to locate market tags attribute for %s' % company_name)

       # Get company size
        try:
            elems = soup.select("span.js-company_size")
            if elems and len(elems) == 1:
                data_obj['company size'] = elems[0].text.strip()
        except:
            print('Unable to locate market tags attribute for %s' % company_name)

        # Get company URL
        try:
            elems = soup.select("div.sidebar a.company_url")
            if elems and len(elems) == 1 and elems[0]['href'][0] != '/':
                data_obj['company url'] = elems[0]['href']
        except:
            print('Unable to locate company URL attribute for %s' % company_name)

        # Get twitter URL
        try:
            elems = soup.select("div.sidebar a.twitter_url")
            if elems and len(elems) == 1 and elems[0]['href'][0] != '/':
                data_obj['twitter url'] = elems[0]['href']
        except:
            print('Unable to locate twitter URL attribute for %s' % company_name)

        # Get facebook URL
        try:
            elems = soup.select("div.sidebar a.facebook_url")
            if elems and len(elems) == 1 and elems[0]['href'][0] != '/':
                data_obj['facebook url'] = elems[0]['href']
        except:
            print('Unable to locate facebook URL attribute for %s' % company_name)

        # Get linkedin URL
        try:
            elems = soup.select("div.sidebar a.linkedin_url")
            if elems and len(elems) == 1 and elems[0]['href'][0] != '/':
                data_obj['linkedin url'] = elems[0]['href']
        except:
            print('Unable to locate linkedin URL attribute for %s' % company_name)

        # Get producthunt URL
        try:
            elems = soup.select("div.sidebar a.producthunt_url")
            if elems and len(elems) == 1 and elems[0]['href'][0] != '/':
                data_obj['producthunt url'] = elems[0]['href']
        except:
            print('Unable to locate producthunt URL attribute for %s' % company_name)

        # Get blog URL
        try:
            elems = soup.select("div.sidebar a.blog_url")
            if elems and len(elems) == 1 and elems[0]['href'][0] != '/':
                data_obj['blog url'] = elems[0]['href']
        except:
            print('Unable to locate blog URL attribute for %s' % company_name)

        # Get founders
        try:
            elems = soup.select("div.founders li.role div.name a.profile-link")
            founders = []
            for elem in elems:
                founders.append({'name': elem.text.strip(), 'url': elem['href']})
            data_obj['founders'] = ';'.join([f['name'] + ' | ' + f['url'] for f in founders])
        except:
            print('Unable to locate founders attribute for %s' % company_name)

        # Get people (investors/team)
        try:
            elems = soup.select("div.group li.role div.name a.profile-link")
            people = []
            for elem in elems:
                people.append({'name': elem.text.strip(), 'url': elem['href']})
            data_obj['people'] = ';'.join([p['name'] + ' | ' + p['url'] for p in people])
        except:
            print('Unable to locate names attribute for %s' % company_name)

        # Get companies
        try:
            elems = soup.select("div.group li.role div.name a.startup-link")
            companies = []
            for elem in elems:
                companies.append({'name': elem.text, 'url': elem['href']})
            data_obj['companies'] = ';'.join([c['name'] + ' | ' + c['url'] for c in companies])
        except:
            print('Unable to locate startups attribute for %s' % company_name)

        # Get portfolio companies
        try:
            elems = soup.select("div.portfolio div.name a.startup-link")
            portfolio_companies = []
            for elem in elems:
                portfolio_companies.append({'name': elem.text, 'url': elem['href']})
            data_obj['portfolio companies'] = ';'.join([c['name'] + ' | ' + c['url'] for c in portfolio_companies])
        except:
            print('Unable to locate portfolio companies attribute for %s' % company_name)

        # Get ___
        if False:
            try:
                pass
            except:
                print('Unable to locate ___ attribute for %s', company_name)

        # Translate Data_Obj to an array of strings:
        data_row = []
        for property in self.angellist_company_properties:
            val = data_obj[property] if property in data_obj else ''
            data_row.append(val)

        return data_row

    def scrape_peron_profile_page(self, letter, company_name, company_url, person_name, url, reliability_score):
        data_obj = None
        rc, txt = self.perform_request(url)
        if rc == 200:
            data_obj = self.parse_person_profile_request_result(letter, txt, company_name, company_url, person_name, url, reliability_score)
            # Translate Data_Obj to an array of strings:
            data_row = []
            for property in self.angellist_person_properties:
                val = data_obj[property] if property in data_obj else ''
                data_row.append(val)
            return rc, data_row
        elif rc == 404:
            print('*** Request on %s returned with %s. Ignoring.' % (person_name, rc))
        else:
            print('*** Request on %s returned with %s. Ignoring.' % (person_name, rc))

        return rc, data_obj

    def scrape_company_profile_page(self, letter, company_name, url):

        data_row = None
        rc, txt = self.perform_request(url)
        if rc == 200:
            data_row = self.parse_company_profile_request_result(letter, txt, company_name, url)
        elif rc == 404:
            print('*** Request on %s returned with %s (%s)' % (company_name, rc, txt))
        else:
            print('*** Request on %s returned with %s (%s)' % (company_name, rc, txt))

        return data_row

    def read_and_scrape_companies_from_anchors(self, letter):

        print("Are you sure you want to run scapring on letter %s (it may override previous data files" % letter)
        answer = input(">>>")
        if answer != 'SURE':
            return

        encoding = 'utf-8'
        path = r'%s\%s_angellist_companies.csv' % (DataHarvester.COMPANY_ANCHOR_FOLDER, letter.upper())
        out_path = r'%s\%s_angellist_companies_detailed.csv' % (DataHarvester.COMPANY_DETAILS_FOLDER, letter.upper())

        # Read last line in file
        if os.path.exists(out_path):
            line = self._tail(out_path, 1)
            elems = line[0].split(',')
            start_row = int(elems[-1])+1
        else:
            start_row = 1

        # Check that file exists
        should_write_header = True if not os.path.isfile(path) else False

        # Create CSV writer for the results
        output_file = open(out_path, 'a', newline='')
        csv_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        if should_write_header:
            csv_writer.writerow(self.angellist_company_properties)
            output_file.flush()

        # Create CSV reader to get company URLs
        csv_reader = csv.reader(codecs.open(path, 'r', encoding), delimiter=';', quotechar='"')
        row_count = 0
        for csv_row in csv_reader:
            row_count += 1
            if row_count < start_row:
                continue

            full_line = ';'.join(csv_row)
            if ";'" in full_line and "';" in full_line:
                csv_row = []
                csv_row.append(full_line[0:full_line.index(";'")])
                csv_row.append(full_line[full_line.index(";'")+2:full_line.index("';")])
                csv_row.append(full_line[full_line.index("';")+2:].split(";")[0])
                csv_row.append(full_line[full_line.index("';")+2:].split(";")[1])
                print('>>> Found problematic row %s - fixed it!' % row_count)
            company_name = csv_row[1]

            if len(csv_row) != 4:
                print('>>>>>>> There is a problem with a line in the CSV file: count: %s, name: %s <<<<<<<' % (row_count, company_name))
                break
            url = csv_row[2]

            # Scrape each URL
            data_row = self.scrape_company_profile_page(letter, company_name, url)

            # Write results to file
            if data_row:
                data_row.append(row_count)
                now_str = str(datetime.datetime.now())
                try:
                    csv_writer.writerow(data_row)
                    output_file.flush()
                    print('%s: Row %s: Done extracting data for %s and writing to file' % (now_str, row_count, company_name))
                except Exception as e:
                    #row_str = str(data_row)
                    print('%s: Unable to write data_row for %s to file (%s)' % (now_str, company_name, e))
                self.num_consecutive_failures = 0
            else:
                print('Failed to write %s to file. Moving on...' % company_name)
                self.num_consecutive_failures += 1
                sleep(60)
                if self.num_consecutive_failures > 20:
                    output_file.close()
                    raise Exception("Too many consecutive failures. Aborting.")

        output_file.close()
        pass

    def read_and_scrape_people_from_companies_file(self, letter):

        the_encoding = 'utf-8'
        path = r'%s\%s_angellist_companies_detailed.csv' % (DataHarvester.COMPANY_DETAILS_FOLDER, letter.upper())
        out_path = r'%s\%s_angellist_persons_detailed.csv' % (DataHarvester.PEOPLE_DETAILS_FOLDER, letter.upper())

        # Read last line in file - find the company
        last_row_read = -1
        if os.path.exists(out_path):
            line = self._tail(out_path, 1)
            elems = line[0].split(',')
            last_row_read = int(elems[-1])
            print('>>> Detected existing file. Seeking last place we stopped... (%s) <<<' % last_row_read)

        # Check that file exists
        should_write_header = True if not os.path.isfile(path) else False

        # Create CSV writer for the results
        output_file = open(out_path, 'a+', newline='', encoding='utf-8')
        csv_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        if should_write_header:
            csv_writer.writerow(self.angellist_person_properties)
            output_file.flush()


        # Create CSV reader to get company URLs
        in_file = codecs.open(path, "r", encoding=the_encoding, errors='ignore')
        csv_reader = csv.reader(in_file, delimiter=',', quotechar='"')
        #csv_reader = csv.reader(codecs.open(path, 'r', encoding), delimiter=',', quotechar='"')
        row_count = 0
        for csv_row in csv_reader:
            row_count += 1
            if row_count <= last_row_read:
                continue

            company_name = csv_row[0]
            company_url = csv_row[1]
            if len(csv_row) != 21:
                print('>>>>>>> There is a problem with a line in the CSV file: Count: %s, Company Name: %s <<<<<<<' % (row_count, company_name))
                break

            # Calculate company's reliability score
            reliability_score = self.calc_company_reliability_score(csv_row)

            # Scrape each person we have his URL
            persons = []
            if len(csv_row[15].strip()) > 0:
                persons.extend(csv_row[15].split(';'))
            if len(csv_row[16].strip()) > 0:
                persons.extend(csv_row[16].split(';'))
            persons_set = set(persons)
            for person_data in list(persons_set):
                person_name = person_data.split('|')[0].strip()
                person_url = person_data.split('|')[1].strip()
                rc, data_row = self.scrape_peron_profile_page(letter, company_name, company_url, person_name, person_url, reliability_score)

                if DataHarvester.UPDATE_COUNT > 0 and row_count % DataHarvester.UPDATE_COUNT == 0:
                    print('Done another batch of 20...')

                # Write results to file
                if rc == 200 and data_row:
                    data_row.append(row_count)  # append the row count so we can continue from last row read if process stops
                    now_str = str(datetime.datetime.now())
                    try:
                        csv_writer.writerow(data_row)
                        output_file.flush()
                        print('%s: Row %s: Done extracting data for %s (%s) and writing to file' % (now_str, row_count, person_name, company_name))
                    except Exception as e:
                        #row_str = str(data_row)
                        print('%s: Unable to write data_row for %s to file (%s)' % (now_str, company_name, e))
                    self.num_consecutive_failures = 0
                elif rc == 404:
                    print('Failed to write %s (%s) to file. (rc=404). Not found. Moving on.' % (person_name, company_name))
                else:
                    print('Failed to write %s (%s) to file. (rc=%s). Moving on...' % (person_name, company_name, rc))
                    self.num_consecutive_failures += 1
                    #sleep(60)
                    if self.num_consecutive_failures > 20:
                        output_file.close()
                        raise Exception("Too many consecutive failures. Aborting.")

        output_file.close()
        pass

if __name__ == '__main__':

    dh = DataHarvester()
    #DataHarvester.rip_company_anchors_from_index_pages()
    #dh.read_and_scrape_companies_from_anchors('U')
    dh.read_and_scrape_people_from_companies_file('Y')

    print('Done harvesting data!')

    pass
