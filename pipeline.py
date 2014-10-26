# encoding=utf8
from distutils.version import StrictVersion
import gzip
import hashlib
import os.path
import random
import shutil
import socket
import sys
import time
import urllib

from seesaw.config import realize, NumberConfigValue
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import GetItemFromTracker, PrepareStatsForTracker, \
    UploadWithTracker, SendDoneToTracker
from seesaw.externalprocess import WgetDownload
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.util import find_executable
import seesaw


# check the seesaw version
if StrictVersion(seesaw.__version__) < StrictVersion("0.7"):
    raise Exception("This pipeline needs seesaw version 0.7 or higher.")


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_LUA will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string
WGET_LUA = find_executable(
    "Wget+Lua",
    ["GNU Wget 1.14.lua.20130523-9a5c"],
    [
        "./wget-lua",
        "./wget-lua-warrior",
        "./wget-lua-local",
        "../wget-lua",
        "../../wget-lua",
        "/home/warrior/wget-lua",
        "/usr/bin/wget-lua"
    ]
)

if not WGET_LUA:
    raise Exception("No usable Wget+Lua found.")


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = "20141026.03"
USER_AGENTS = [
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30618; MAXTHON 2.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.103 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36',
    ]
TRACKER_ID = 'twitpic2'
TRACKER_HOST = 'tracker.archiveteam.org'

ACCEPT_LANGUAGE_HEADERS = [
    "Accept-Language: en-CA,en;q=0.8,en-US;q=0.6,en-GB;q=0.4",
    "Accept-Language: en-US,en;q=0.8,en-CA;q=0.6,en-GB;q=0.4",
    "Accept-Language: en-GB,en;q=0.8,en-US;q=0.6,en-CA;q=0.4",
    "Accept-Language: en-US,en;q=0.8,en-GB;q=0.6,en-CA;q=0.4",
    "Accept-Language: en-US,en;q=0.8,en-US;q=0.6,en-US;q=0.4",
    ]


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "CheckIP")
        self._counter = 0

    def process(self, item):
        # NEW for 2014! Check if we are behind firewall/proxy
        # Check if we are banned from twitpic
        check = urllib.urlopen('http://twitpic.com/2')
        if check.getcode() == 403:
            item.log_output('You are banned from Twitpic! Please try to use an other IP.')
            raise Exception('You are banned from Twitpic! Please try to use an other IP.')

        if self._counter <= 0:
            item.log_output('Checking IP address.')
            ip_set = set()

            ip_set.add(socket.gethostbyname('twitter.com'))
            ip_set.add(socket.gethostbyname('facebook.com'))
            ip_set.add(socket.gethostbyname('youtube.com'))
            ip_set.add(socket.gethostbyname('microsoft.com'))
            ip_set.add(socket.gethostbyname('icanhas.cheezburger.com'))
            ip_set.add(socket.gethostbyname('archiveteam.org'))

            if len(ip_set) != 6:
                item.log_output('Got IP addresses: {0}'.format(ip_set))
                item.log_output(
                    'You are behind a firewall or proxy. That is a big no-no!')
                raise Exception(
                    'You are behind a firewall or proxy. That is a big no-no!')

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 10
        else:
            self._counter -= 1


class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, "PrepareDirectories")
        self.warc_prefix = warc_prefix

    def process(self, item):
        item_name = item["item_name"]
        escaped_item_name = item_name.replace(':', '_').replace('/', '_')
        item['escaped_item_name'] = escaped_item_name

        dirname = "/".join((item["data_dir"], escaped_item_name))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item["item_dir"] = dirname
        item["warc_file_base"] = "%s-%s-%s" % (self.warc_prefix, escaped_item_name,
                                               time.strftime("%Y%m%d-%H%M%S"))

        open("%(item_dir)s/%(warc_file_base)s.warc.gz" % item, "w").close()


class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "MoveFiles")

    def process(self, item):
        # NEW for 2014! Check if wget was compiled with zlib support
        if os.path.exists("%(item_dir)s/%(warc_file_base)s.warc" % item):
            raise Exception('Please compile wget with zlib support!')

        os.rename("%(item_dir)s/%(warc_file_base)s.warc.gz" % item,
                  "%(data_dir)s/%(warc_file_base)s.warc.gz" % item)

        # This line special only for scraping. Delete this in other scripts
        # when copying and pasting:
        os.rename("%(item_dir)s/twitpic2-scrape-%(escaped_item_name)s.txt.gz" % item,
                  "%(data_dir)s/twitpic2-scrape-%(escaped_item_name)s.txt.gz" % item)

        shutil.rmtree("%(item_dir)s" % item)


def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha1(in_file.read()).hexdigest()


CWD = os.getcwd()
PIPELINE_SHA1 = get_hash(os.path.join(CWD, 'pipeline.py'))
LUA_SHA1 = get_hash(os.path.join(CWD, 'twitpic.lua'))


def stats_id_function(item):
    # NEW for 2014! Some accountability hashes and stats.
    d = {
        'pipeline_hash': PIPELINE_SHA1,
        'lua_hash': LUA_SHA1,
        'python_version': sys.version,
        }

    return d


ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"

def int_to_str(num, alphabet=ALPHABET):
    """Encode a number in Base X

    `num`: The number to encode
    `alphabet`: The alphabet to use for encoding
    """
    # http://stackoverflow.com/a/1119769/1524507
    if (num == 0):
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        rem = num % base
        num = num // base
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)


def str_to_int(string, alphabet=ALPHABET):
    """Decode a Base X encoded string into the number

    Arguments:
    - `string`: The encoded string
    - `alphabet`: The alphabet to use for encoding
    """
    base = len(alphabet)
    strlen = len(string)
    num = 0

    idx = 0
    for char in string:
        power = (strlen - (idx + 1))
        num += alphabet.index(char) * (base ** power)
        idx += 1

    return num


class WgetArgs(object):
    def realize(self, item):
        wget_args = [
            WGET_LUA,
            "-U", random.choice(USER_AGENTS),
            "-nv",
            "--lua-script", "twitpic.lua",
            "-o", ItemInterpolation("%(item_dir)s/wget.log"),
            "--no-check-certificate",
            "--output-document", ItemInterpolation("%(item_dir)s/wget.tmp"),
            "--truncate-output",
            "-e", "robots=off",
            # "-w", "1",
            "--no-cookies",
            "--rotate-dns",
            # "--recursive", "--level=inf",
            "--no-parent",
            # "--page-requisites",
            "--timeout", "30",
            "--tries", "inf",
            "--span-hosts",
            "--waitretry", "30",
            "--domains", "twitpic.com,cloudfront.net,twimg.com,amazonaws.com",
            "--warc-file", ItemInterpolation("%(item_dir)s/%(warc_file_base)s"),
            "--warc-header", "operator: Archive Team",
            "--warc-header", "twitpic2-dld-script-version: " + VERSION,
            "--warc-header", ItemInterpolation("twitpic2-user: %(item_name)s"),
            "--header", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "--header", "DNT: 1",
            "--header", random.choice(ACCEPT_LANGUAGE_HEADERS),
            ]

        item_name = item['item_name']
        assert ':' in item_name
        item_type, item_value = item_name.split(':', 1)

        item['item_type'] = item_type
        item['item_value'] = item_value

        assert item_type in ('image', )

        if item_type == 'image':
            start_id, end_id = item_value.split(':', 1)
            start_num = str_to_int(start_id)
            end_num = str_to_int(end_id)

            for num in range(start_num, end_num + 1):
                twitpic_name = int_to_str(num)
                url = 'http://twitpic.com/{0}'.format(twitpic_name)
                wget_args.append(url)

        else:
            raise Exception('Unknown item')

        if 'bind_address' in globals():
            wget_args.extend(['--bind-address', globals()['bind_address']])
            print('')
            print('*** Wget will bind address at {0} ***'.format(
                globals()['bind_address']))
            print('')

        return realize(wget_args, item)


class ProcessScrapeFile(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "ProcessScrapeFile")

    def process(self, item):
        text_file_filename = "%(item_dir)s/twitpic2-scrape-%(escaped_item_name)s.txt" % item
        gzip_filename = "%(item_dir)s/twitpic2-scrape-%(escaped_item_name)s.txt.gz" % item

        if not os.path.exists(text_file_filename):
            # Create empty file since rsync upload task expects this file
            open(gzip_filename, 'wb').close()
        else:
            text_file = open(text_file_filename, 'rb')
            gzip_file = gzip.GzipFile(gzip_filename, 'wb')

            shutil.copyfileobj(text_file, gzip_file)
            text_file.close()
            gzip_file.close()


###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title="Twitpic 2",
    project_html="""
        <img class="project-logo" alt="Project logo" src="http://archiveteam.org/images/b/b3/Quitpic.png" height="50px" title=""/>
        <h2>twitpic.com <span class="links"><a href="http://twitpic.com/">Website</a> &middot; <a href="http://tracker.archiveteam.org/twitpic2/">Leaderboard</a></span></h2>
        <p>Saving TwitPic's smoldering remains.</p>
        <!--<p class="projectBroadcastMessage"></p>-->
    """,
    # utc_deadline=datetime.datetime(2014, 9, 25, 23, 59, 0)
)

pipeline = Pipeline(
    CheckIP(),
    GetItemFromTracker("http://%s/%s" % (TRACKER_HOST, TRACKER_ID), downloader,
                       VERSION),
    PrepareDirectories(warc_prefix="twitpic2"),
    WgetDownload(
        WgetArgs(),
        max_tries=2,
        accept_on_exit_code=[0, 4, 7, 8],
        env={
            "item_dir": ItemValue("item_dir"),
            "item_value": ItemValue("item_value"),
            "item_type": ItemValue("item_type"),
            "escaped_item_name": ItemValue("escaped_item_name"),
            "downloader": downloader
        }
    ),
    ProcessScrapeFile(),
    PrepareStatsForTracker(
        defaults={"downloader": downloader, "version": VERSION},
        file_groups={
            "data": [
                ItemInterpolation("%(item_dir)s/%(warc_file_base)s.warc.gz"),
                ItemInterpolation("%(item_dir)s/twitpic2-scrape-%(escaped_item_name)s.txt.gz")
            ]
        },
        id_function=stats_id_function,
        ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=4, default="1",
                                      name="shared:rsync_threads", title="Rsync threads",
                                      description="The maximum number of concurrent uploads."),
                    UploadWithTracker(
                        "http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
                        downloader=downloader,
                        version=VERSION,
                        files=[
                            ItemInterpolation("%(data_dir)s/%(warc_file_base)s.warc.gz"),
                            ItemInterpolation("%(data_dir)s/twitpic2-scrape-%(escaped_item_name)s.txt.gz")
                        ],
                        rsync_target_source_path=ItemInterpolation("%(data_dir)s/"),
                        rsync_extra_args=[
                            "--recursive",
                            "--partial",
                            "--partial-dir", ".rsync-tmp",
                            ]
                    ),
                    ),
    SendDoneToTracker(
        tracker_url="http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue("stats")
    )
)
