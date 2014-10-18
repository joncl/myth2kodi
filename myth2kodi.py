#! /usr/bin/env python
# -- coding: utf-8 --

import httplib

import os
import xml.etree.cElementTree as ET
from lxml import etree as ET2
import xml.dom.minidom as dom
import urllib2
import re
import argparse
import zipfile
from PIL import Image
import cStringIO
import json
import sys
import MySQLdb
import time
import subprocess
import config


BASE_URL = "http://" + config.hostname + ":" + config.host_port
THIS_DIR = os.getcwd()
db = None
log_content = ''


def timestamp():
    now = time.time()
    localtime = time.localtime(now)
    ms = '%03d' % int((now - int(now)) * 1000)
    return time.strftime('%Y%_m%d_%H%M%S', localtime) + '.' + ms


def log_info(content):
    log(content, 'INFO')


def log_error(content):
    log(content, 'ERROR')


def log(content, type):
    global log_content
    log_content = '{}{} {}: {}\n'.format(log_content, timestamp(), type.rjust(5),
                                         content.replace('^', ' ' * 31))


def get_db_cursor():
    global db
    if db is None:
        db = MySQLdb.connect('localhost', config.db_user, config.db_passwd, config.db_name)
    return db.cursor()


def close_db():
    global db
    if db is not None:
        db.cursor().close()
        db.close()


def log_missing_inet_ref(title):
    return


def prettify(xml_element):
    """
    format xml
    :param xml_element: xml element to format
    :return: formatted xml element
    """
    rough_string = ET.tostring(xml_element, 'utf-8')
    reparsed = dom.parseString(rough_string)
    return reparsed.toprettyxml(indent="\t")


def write_series_nfo(directory, title, rating, votes, plot, id, genre_list, premiered, studio, date_added):
    """
    write nfo file for series
    :param directory:
    :param title:
    :param rating:
    :param votes:
    :param plot:
    :param id:
    :param genre_list:
    :param premiered:
    :param studio:
    :param date_added:
    :return:
    """
    log_info('Writing series nfo file...')
    root = ET2.Element('tvshow')
    title_element = ET2.SubElement(root, 'title')
    title_element.text = title
    title_element = ET2.SubElement(root, 'rating')
    title_element.text = rating
    title_element = ET2.SubElement(root, 'votes')
    title_element.text = votes
    title_element = ET2.SubElement(root, 'plot')
    title_element.text = plot
    title_element = ET2.SubElement(root, 'id')
    title_element.text = id

    # add genres
    if genre_list is not None and '|' in genre_list:
        genre_list = genre_list.strip('|').split('|')
        # print 'genre_list: ' + str(genre_list)
        for genre in genre_list:
            # print 'genre: ' + genre
            title_element = ET2.SubElement(root, 'genre')
            title_element.text = genre
    else:
        title_element = ET2.SubElement(root, 'genre')
        title_element.text = genre_list

    title_element = ET2.SubElement(root, 'premiered')
    title_element.text = premiered
    title_element = ET2.SubElement(root, 'studio')
    title_element.text = studio
    title_element = ET2.SubElement(root, 'dateadded')
    title_element.text = date_added
    tree = ET2.ElementTree(root)
    tree.write(os.path.join(directory, 'tvshow.nfo'), pretty_print=True, encoding='UTF-8', xml_declaration=True)
    return


def write_episode_nfo(title, season, episode, plot, airdate, playcount, base_link_file):
    """
    write nfo file for episode
    :param title:
    :param season:
    :param episode:
    :param plot:
    :param airdate:
    :param base_link_file:
    :return:
    """
    if args.add is not None:
        print '    ' + base_link_file
    root = ET2.Element('episodedetails')
    title_element = ET2.SubElement(root, 'title')
    title_element.text = title
    season_element = ET2.SubElement(root, 'season')
    season_element.text = season
    episode_element = ET2.SubElement(root, 'episode')
    episode_element.text = episode
    plot_element = ET2.SubElement(root, 'plot')
    plot_element.text = plot
    airdate_element = ET2.SubElement(root, 'aired')
    airdate_element.text = airdate
    playcount_element = ET2.SubElement(root, 'playcount')
    playcount_element.text = playcount
    tree = ET2.ElementTree(root)
    tree.write(base_link_file + '.nfo', pretty_print=True, encoding='UTF-8', xml_declaration=True)
    return


def write_comskip(base_link_file, mark_dict):
    if not len(mark_dict):
        return

    c = 'FILE PROCESSING COMPLETE\r\n'
    c = c + '------------------------\r\n'
    for start, end in mark_dict.iteritems():
        c = c + '{} {}\r\n'.format(str(start), str(end))

    f = open(base_link_file + '.txt', 'a')
    f.write(c)
    f.close()


def series_nfo_exists(directory):
    """
    check if series nfo file tvshow.nfo exists
    :param directory: directory to check for nfo file
    :return: True exists, or False does not exist
    """
    return os.path.exists(os.path.join(directory, 'tvshow.nfo'))


def check_recordings_dirs():
    """
    checks mythtv recording directories listed in config.py under myth_recording_dir
    """
    for myth_recording_dir in config.mythtv_recording_dirs[:]:
        if os.path.exists(myth_recording_dir) is not True:
            print myth_recording_dir + " is not a valid path. Aborting"


def get_base_filename_from(path):
    """
    return the base filename
    :param path:
    :return: base filename
    """
    return os.path.splitext(os.path.basename(path))[0]


def get_series_id(inetref):
    """
    regex just the id # from a ttvdb or tmdb internet reference
    :param inetref: internet reference stored in MythTV
    :return: series id #
    """
    return re.findall('[\d]+$', inetref)[0]


def download_file(file_url, target_file='', return_response=False):
    try:
        req = urllib2.Request(file_url)
        response = urllib2.urlopen(req)
        if return_response is True:
            return response
        output = open(target_file, 'wb')
        output.write(response.read())
        output.close()
    except urllib2.HTTPError, e:
        log_error('HTTPError = ' + str(e.code))
        return False
    except urllib2.URLError, e:
        log_error('URLError = ' + str(e.reason))
        return False
    except httplib.HTTPException, e:
        log_error('HTTPException')
        return False
    except Exception:
        import traceback

        log_error('generic exception: ' + traceback.format_exc())
        return False
    else:
        return True


def new_series_from_ttvdb(title, title_safe, inetref, category, directory):
    # store series zip from thetvdb.com
    """
    create a new show from a ttvdb id
    :param title:
    :param title_safe:
    :param inetref:
    :param category:
    :param directory:
    :return: True: success, False: error
    """
    ttvdb_base_url = 'http://www.thetvdb.com/'
    series_id = get_series_id(inetref)
    series_zip_file = os.path.join(config.ttvdb_zips_dir, title_safe + '_' + series_id + '.zip')
    if not os.path.exists(series_zip_file):
        # zip does not exist, download it
        # print '    downloading ttvdb zip file...'
        log_info('TTVDB zip does not exist, downloading ttvdb zip file to: ' + series_zip_file)
        ttvdb_zip_file = ttvdb_base_url + 'api/' + config.ttvdb_key + '/series/' + series_id + '/all/en.zip'
        download_file(ttvdb_zip_file, series_zip_file)
        # urllib.urlretrieve(ttvdb_zip_file, series_zip_file)

    # extract poster, banner, and fanart urls
    # print '    ttvdb zip exists, reading xml contents...'
    log_info('ttvdb zip exists, reading xml contents...')
    z = zipfile.ZipFile(series_zip_file, 'r')
    for name in z.namelist():
        if name == 'en.xml':
            z.extract(name, '/tmp/')
            break
    if not os.path.exists('/tmp/en.xml'):
        print '    en.xml not found in series zip file at /tmp/en.xml'
        log_error('en.xml not found in series zip file at /tmp/en.xml')
        return False
    else:
        log_info('Reading en.xml...')
        tree = ET.parse('/tmp/en.xml')
        series_data = tree.getroot()
        series = series_data.find('Series')

        rating = series.find('Rating').text
        votes = series.find('RatingCount').text
        plot = series.find('Overview').text
        id = series.find('id').text
        premiered = series.find('FirstAired').text
        studio = series.find('Network').text
        date_added = series.find('added').text

        # assemble genre string
        genre_list = series.find('Genre').text
        if genre_list is not None and '|' in genre_list:
            genre_list = genre_list.strip('|')
            if category not in genre_list:
                genre_list += '|' + category
            genre_list = genre_list.strip('|')

        # print 'genre_list: ' + genre_list

        write_series_nfo(directory, title, rating, votes, plot, id, genre_list, premiered, studio, date_added)

        # copy poster, banner, and fanart to link dir
        log_info('Retrieving poster, fanart, and banner...')
        ttvdb_banners_url = ttvdb_base_url + 'banners/'
        poster_url = ttvdb_banners_url + series.find('poster').text
        banner_url = ttvdb_banners_url + series.find('banner').text
        fanart_url = ttvdb_banners_url + series.find('fanart').text

        log_info('downloading poster from: ' + poster_url)
        if not download_file(poster_url, os.path.join(directory, 'poster.jpg')):
            return False

        log_info('downloading banner from: ' + banner_url)
        if not download_file(banner_url, os.path.join(directory, 'banner.jpg')):
            return False

        log_info('downloading fanart from: ' + fanart_url)
        if not download_file(fanart_url, os.path.join(directory, 'fanart.jpg')):
            return False

        # urllib.urlretrieve(ttvdb_banners_url + poster, os.path.join(directory, 'poster.jpg'))
        # urllib.urlretrieve(ttvdb_banners_url + banner, os.path.join(directory, 'banner.jpg'))
        # urllib.urlretrieve(ttvdb_banners_url + fanart, os.path.join(directory, 'fanart.jpg'))
        return True


def new_series_from_tmdb(title, inetref, category, directory):
    """
    create a new show from a tmdb id
    :param title:
    :param inetref:
    :param category:
    :param directory:
    :return: True: success, False: error
    """
    api_url = 'http://api.themoviedb.org/3'
    headers = {'Accept': 'application/json'}

    request = urllib2.Request('{url}/configuration?api_key={key}'.format(url=api_url, key=config.tmdb_key),
                              headers=headers)
    cr = json.loads(urllib2.urlopen(request).read())

    # base url
    base_url = cr['images']['base_url']
    poster_sizes = cr['images']['poster_sizes']
    backdrop_sizes = cr['images']['backdrop_sizes']
    """
        'sizes' should be sorted in ascending order, so
            max_size = sizes[-1]
        should get the largest size as well.
    """

    def size_str_to_int(x):
        return float("inf") if x == 'original' else int(x[1:])

    # max_size
    max_poster_size = max(poster_sizes, key=size_str_to_int)
    max_backdrop_size = max(backdrop_sizes, key=size_str_to_int)

    series_id = get_series_id(inetref)
    request = urllib2.Request('{url}/movie/{id}?api_key={key}'.format(url=api_url, id=series_id, key=config.tmdb_key))
    mr = json.loads(urllib2.urlopen(request).read())

    # #### POSTER #####
    log_info('Getting path to poster...')
    poster_path = mr['poster_path']
    poster_url = '{0}{1}{2}'.format(base_url, max_poster_size, poster_path)
    # #### FANART #####
    log_info('Getting path to fanart...')
    backdrop_path = mr['backdrop_path']
    backdrop_url = '{0}{1}{2}'.format(base_url, max_backdrop_size, backdrop_path)

    # print '    poster_path: ' + poster_path
    # print '    backdrop_path: ' + backdrop_path

    # request = Request('{url}/movie/{id}/images?api_key={key}'.format(url=api_url, id=series_id, key=config.tmdb_key))
    # ir = json.loads(urlopen(request).read())

    # save images
    # urllib.urlretrieve(poster_url, os.path.join(directory, 'poster.jpg'))
    # urllib.urlretrieve(backdrop_url, os.path.join(directory, 'fanart.jpg'))
    if download_file(poster_url, os.path.join(directory, 'poster.jpg')) is False:
        return False
    if download_file(backdrop_url, os.path.join(directory, 'fanart.jpg')) is False:
        return False

    # #### BANNER #####
    # make a 758 x 140 banner from poster
    log_info('Making 758 x 140 banner image from poster...')
    response = download_file(poster_url, '', True)
    if response is None:
        return False
    img = Image.open(cStringIO.StringIO(response.read()))
    # print 'w: ' + str(img.size[0]) + ' h: ' + str(img.size[1])
    banner_ratio = 758 / float(140)
    # shift crop down by 200 pixels
    box = (0, 220, img.size[0], int(round((img.size[0] / banner_ratio))) + 220)
    img = img.crop(box).resize((758, 140), Image.ANTIALIAS)
    img.save(os.path.join(directory, 'banner.jpg'))

    # print mr
    # print mr['title']
    # print mr['runtime']

    rating = str(mr['vote_average'])
    votes = str(mr['vote_count'])
    plot = str(mr['overview'])
    id = str(mr['id'])
    premiered = str(mr['release_date'])
    studio = ''
    date_added = ''

    # assemble genre string
    # print mr['genres']
    genres = mr['genres']
    if genres is not None:
        genre_list = ''
        for genre in genres:
            name = genre['name']
            if name is not None:
                if not name.lower() == category.lower():
                    genre_list += name + '|'
        if category is not None:
            genre_list += category
        genre_list = str(genre_list).strip('|')

    # print 'rating: ' + rating
    # print 'votes: ' + votes
    # print 'plot: ' + plot
    # print 'id: ' + id
    # print 'premiered: ' + premiered
    # print 'genre_list: ' + genre_list

    write_series_nfo(directory, title, rating, votes, plot, id, genre_list, premiered, studio, date_added)
    return True


parser = argparse.ArgumentParser(__file__,
                                 description='myth2kodi... A script to enable viewing of MythTV recordings in XBMC(Kodi)\n' +
                                             '\n' +
                                             'On GitHub: https://github.com/joncl/myth2kodi\n' +
                                             '\n' +
                                             'NOTES:\n' +
                                             ' - At least one argument is required.\n' +
                                             ' - Use --scan-all or --comskip-all separately to either scan for new MythTV recordings or\n' +
                                             '   scan for commercials in existing MythTV recordings already linked.\n' +
                                             ' - Create a MythTV user job with -add <path to MythTV mpg> to add a new MythTV recording.\n' +
                                             '   The new recording will be scanned for commercials after it is added.\n',
                                 formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('--add', dest='add', action='store', metavar='<path to mpg file>',
                    help="Full path to file name of MythTV recording, used for a MythTV user job upon 'Recording Finished'.")
parser.add_argument('--add-all', dest='add_all', action='store_true', default=False,
                    help='Add all MythTV recordings that are missing.')
parser.add_argument('--show-all', dest='show_all', action='store_true', default=False,
                    help='Show all MythTV recordings that are missing. This will not write any new symlinks or files.')
parser.add_argument('--print-new', dest='print_new', action='store_true', default=False,
                    help='Print new recordings not yet linked, mostly used for testing purposes.')
parser.add_argument('--show-xml', dest='source_xml', action='store', metavar='<mpg file name match>',
                    help='Show source xml for an episode that contains any portion of the given mpg file name.')
parser.add_argument('--comskip', dest='comskip', action='store', metavar='<path to mpg file>',
                    help="Full path to file name of MythTV recording, used to comskip just a single recording.")
parser.add_argument('--comskip-all', dest='comskip_all', action='store_true', default=False,
                    help='Run comskip on all video files found recursively in the "symlinks_dir" path from config.py.')
parser.add_argument('--comskip-off', dest='comskip_off', action='store_true', default=False,
                    help='Turn off comskip when adding a single recording with --add.')
parser.add_argument('--match-title', dest='match_title', action='store', metavar='<title match>',
                    help='Process only series matching any portion of the given title')
parser.add_argument('--print-config', dest='print_config', action='store_true',
                    help='Prints all the config variables and values in the config.py file.')

# TODO: handle arguments refresh nfos
# TODO: clean up symlinks, nfo files, and directories when MythTV recordings are deleted
# parser.add_argument('-r', '--refresh-nfos', dest='refresh_nfos', action='store_true', default=False,
# help='refresh all nfo files')
# parser.add_argument('-c', '--clean', dest='clean', action='store_true', default=False,
# help='remove all references to deleted MythTV recordings')
# parser.add_argument('--rebuild-library', dest='rebuild_library', action='store_true', default=False,
# help='rebuild library from existing links')

if len(sys.argv) == 1:
    parser.error('At lease one argument is required. Use -h or --help for details.')
    sys.exit(1)

args = parser.parse_args()
# print args.source_xml


def print_config():
    print ''
    print 'config.py:'
    print '    hostname:            ' + str(config.hostname)
    print '    host_port:           ' + str(config.host_port)
    print '    myth_recording_dirs: ' + str(config.mythtv_recording_dirs)
    print '    symlinks_dir:        ' + str(config.symlinks_dir)
    print '    ttvdb_key:           ' + str(config.ttvdb_key)
    print '    ttvdb_zips_dir:      ' + str(config.ttvdb_zips_dir)
    print '    tmdb_key:            ' + str(config.tmdb_key)
    print '    db_user:             ' + str(config.db_user)
    print '    db_passwd:           ' + str(config.db_passwd)
    print '    db_name:             ' + str(config.db_name)
    print ''


def get_recorded_list():
    """
    get recorded list from mythtv database

    :return: xml parsed recorded list
    """
    # print "Looking up from MythTV: " + url + '/Dvr/GetRecordedList'
    tree = ET.parse(urllib2.urlopen(BASE_URL + '/Dvr/GetRecordedList'))
    # print prettify(tree.getroot())
    # sys.exit(0)
    return tree.getroot()


def check_args():
    if args.print_new is True:
        print 'MYTHTV RECORDINGS NOT LINKED:'

    if args.add is not None:
        print 'ADDING NEW RECORDING: ' + args.add


recorded_list = get_recorded_list()
check_args()


def write_log(msg=None):
    global log_content
    log_content += '\n\n'
    f = open('myth2kodi.log', 'w')
    if msg is None:
        f.write(log_content)
    else:
        f.write(log_content + '\n\n' + str(msg))
    f.close()


def comskip_file(path, file):
    if file.lower().endswith('.mpg'):
        base_file = os.path.splitext(file)[0]
        txt_file = os.path.join(path, base_file + '.txt')
        log_file = os.path.join(path, base_file + '.log')
        logo_file = os.path.join(path, base_file + '.logo.txt')
        edl_file = os.path.join(path, base_file + '.edl')

        # if txt file exists, then skip this mpg
        if os.path.exists(txt_file):
            return

        # run comskip
        base_comskip_dir = os.path.dirname(config.comskip_exe)
        os.chdir(base_comskip_dir)
        subprocess.call('wine {} "{}"'.format(config.comskip_exe, os.path.join(path, file)), shell=True)
        os.chdir(THIS_DIR)

        # remove extra files
        if os.path.exists(log_file):
            os.remove(log_file)
        if os.path.exists(logo_file):
            os.remove(logo_file)
        if os.path.exists(edl_file):
            os.rename(edl_file, edl_file + '.bak')


def comskip_all():
    for root, dirs, files in os.walk(config.symlinks_dir):
        # print root
        # path = root.split('/')
        # print path
        # print (len(path) - 1) *'---' , os.path.basename(root)
        for file in files:
            comskip_file(root, file)


def read_recordings():
    """
    read MythTV recordings

    """
    series_lib = []
    series_new_count = 0
    episode_count = 0
    episode_new_count = 0
    special_count = 0
    special_new_count = 0
    image_error_lib = []

    for program in recorded_list.iter('Program'):
        is_special = False

        # print the program for testing
        # print Prettify(program)

        # check if we're adding a new file by comparing the current file name to the argument file name
        file_name = program.find('FileName').text
        base_file_name = get_base_filename_from(file_name)
        if args.add is not None:
            if not base_file_name == get_base_filename_from(args.add):
                continue
            log_info('Adding new file by "--add" argument: {}'.format(args.add))

        # collect program attributes
        title = program.find('Title').text
        season = program.find('Season').text
        episode = program.find('Episode').text.zfill(2)
        airdate = program.find('Airdate').text
        plot = program.find('Description').text
        inetref = program.find('Inetref').text
        category = program.find('Category').text
        record_date = re.findall('\d*-\d*-\d*', program.find('StartTime').text)[0]
        program_id = program.find('ProgramId').text
        chan_id = program.find('Channel/ChanId').text
        start_time = program.find('StartTime').text.replace('T', ' ').replace('Z', '')

        # check if we are matching on a title
        if args.match_title is not None:
            if args.match_title not in title:
                continue

        # print program xml if --show-xml arg is given
        if args.source_xml is not None:
            if str(args.source_xml) in base_file_name:
                print prettify(program)
                sys.exit(0)
            else:
                continue

        file_extension = file_name[-4:]
        log_info('PROCESSING PROGRAM:\n^title: {}\n^filename: {}\n^program id: {}'.format(title,
                                                                                          base_file_name + file_extension,
                                                                                          program_id))

        # be sure we have an inetref
        if inetref is None or inetref == '':
            log_error('MISSING INETREF!: ' + title)
            continue

        # be sure we have a program_id
        if program_id is None or program_id == '':
            log_error('MISSING PROGRAMID! ' + title)
            continue

        try:
            # lookup watched flag from db
            log_info('Looking up watched flag from db')
            cursor = get_db_cursor()
            sql = 'select watched from recorded where programid = "{}";'.format(program_id)
            cursor.execute(sql)
            results = cursor.fetchone()
            playcount = re.findall('\d', str(results))[0]

            # lookup commercial markers
            log_info('Looking up commercial markers')
            mark_list = []
            mark_types = ( '4', '5')
            for mark_type in mark_types:
                sql = 'select mark from recordedmarkup where starttime = "{}" and chanid = "{}" and type = "{}"'.format(
                    start_time, chan_id, mark_type)
                cursor.execute(sql)
                results = cursor.fetchall()
                l = []
                for result in results:
                    l.append(result[0])
                mark_list.append(l)

            mark_dict = dict(zip(mark_list[0], mark_list[1]))

        except(AttributeError, MySQLdb.OperationalError):
            log_error('Unable to fetch data!')
            print AttributeError.message
            print 'Error: unable to fetch data'

        # parse show name for file system safe name
        title_safe = re.sub('[\[\]/\\;><&*:%=+@!#^()|?]', '', title)
        title_safe = re.sub(' +', '_', title_safe)

        # form the file name
        episode_name = title_safe + " - " + season + "x" + episode + " - " + base_file_name
        # if it's a special...
        if season.zfill(2) == "00" and episode == "00":
            episode_name = episode_name + " - " + record_date
            airdate = record_date  # might be needed so specials get sorted with recognized episodes
            is_special = True

        # set target link dir
        target_link_dir = os.path.join(config.symlinks_dir, title_safe)
        link_file = os.path.join(target_link_dir, episode_name) + file_extension
        # print 'LINK FILE = ' + link_file

        # check if we're running comskip on just one recording
        if args.comskip is not None:
            if base_file_name == get_base_filename_from(args.comskip):
                comskip_file(os.path.dirname(link_file), os.path.basename(link_file))
                log_info('Running comskip on ' + args.comskip)
                break

        # update series library and count
        if not series_lib or title_safe not in series_lib:
            series_lib.append(title_safe)

        # update episode/special library and count
        if is_special is True:
            special_count += 1
            # if not special_lib or base_file_name not in special_lib:
            # special_lib.append(base_file_name);
        else:
            episode_count += 1
        # if not episode_lib or base_file_name not in episode_lib:
        # episode_lib.append(base_file_name)

        # skip if link already exists
        if os.path.exists(link_file) or os.path.islink(link_file):
            # print "Symlink " + link_file + " already exists.  Skipping."
            log_info('Symlink ' + link_file + ' already exists.  Skipping.')
            continue

        # find source directory, and if not found, skip it because it's an orphaned recording!
        source_dir = None
        for mythtv_recording_dir in config.mythtv_recording_dirs[:]:
            source_file = os.path.join(mythtv_recording_dir, base_file_name) + file_extension
            if os.path.isfile(source_file):
                source_dir = mythtv_recording_dir
                break

        if source_dir is None:
            # could not find file!
            # print ("Cannot create symlink for " + episode_name + ", no valid source directory.  Skipping.")
            log_error('Cannot create symlink for ' + episode_name + ', no valid source directory.  Skipping.')
            continue

        # this is a new recording, so check if we're just getting a list of them for now
        if args.print_new is True:
            print '   ' + source_file
            continue


        # process new show if found
        if not os.path.exists(target_link_dir):  # and 'Johnny' in title_safe:
            if args.show_all is False:
                os.makedirs(target_link_dir)
            series_new_count += 1

            # branch on inetref type
            result = True
            if 'ttvdb' in inetref:
                print 'Adding new series from TTVDB: ' + title
                log_info('Adding new series from TTVDB: ' + title)
                if args.show_all is False:
                    result = new_series_from_ttvdb(title, title_safe, get_series_id(inetref), category, target_link_dir)
            if 'tmdb' in inetref:
                print 'Adding new series from TMDB: ' + title
                log_info('Adding new series from TMDB: ' + title)
                if args.show_all is False:
                    result = new_series_from_tmdb(title, get_series_id(inetref), category, target_link_dir)

            # print "RESULT: " + str(result)
            if result is False:
                image_error_lib.append(link_file)
                print 'ERROR processing image for link_file: ' + link_file
                log_error('Error processing image for link_file: ' + link_file)
                continue

        # create symlink
        # print "Linking " + source_file + " ==> " + link_file
        if args.show_all is False:
            os.symlink(source_file, link_file)
        log_info('Linking ' + source_file + ' ==> ' + link_file)

        # write the episode nfo and comskip file
        path = os.path.splitext(link_file)[0]
        if args.show_all is False:
            write_episode_nfo(title, season, episode, plot, airdate, playcount, path)

        # commercial skipping didn't work reliably using frames markers from the mythtv database as of .27
        # keep the code here anyway for later reference
        # write_comskip(path, mark_dict)

        # count new episode or special
        if is_special is True:
            special_new_count += 1
        else:
            episode_new_count += 1

        # if adding a new recording with --add, comskip it, and then stop looking
        if args.add is not None:
            if args.comskip_off is False:
                # using comskip for commercial detection: http://www.kaashoek.com/comskip/
                comskip_file(os.path.dirname(link_file), os.path.basename(link_file))
            break

    print ''
    print '   --------------------------------'
    print '   |         |  Series:   ' + str(len(series_lib))
    print '   |  Total  |  Episodes: ' + str(episode_count)
    print '   |         |  Specials: ' + str(special_count)
    print '   |-------------------------------'
    print '   |         |  Series:   ' + str(series_new_count)
    print '   |   New   |  Episodes: ' + str(episode_new_count)
    print '   |         |  Specials: ' + str(special_new_count)
    print '   --------------------------------'
    print '   |  Errors: ' + str(len(image_error_lib))
    if len(image_error_lib) > 0:
        print ''
        print 'Error processing images for these link_files:'
        print ''
        for lf in image_error_lib:
            print lf
    else:
        print '   --------------------------------'
    print ''

    return not (len(image_error_lib) > 0 or 'ERROR' in log_content)


try:
    success = True
    if args.print_config is True:
        print_config()
    elif args.comskip_all is True:
        comskip_all()
    elif args.add_all is True or args.show_all is True or args.add is not None:
        success = read_recordings()
    if success is not True:
        sys.exit(1)
except Exception, e:
    close_db()
    write_log(e)
    sys.exit(1)
else:
    close_db()
    write_log()
    sys.exit(0)
