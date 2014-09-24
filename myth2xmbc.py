#! /usr/bin/env python

import os
import xml.etree.cElementTree as ET
from lxml import etree as ET2
import xml.dom.minidom as dom
import urllib
import re
import argparse
import zipfile
from PIL import Image
import cStringIO
from urllib2 import Request, urlopen
import json
import sys

import config


BASE_URL = "http://" + config.hostname + ":" + config.host_port


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


def write_series_nfo(directory, title, rating, votes, plot, id, genre, premiered, studio, dateadded):
    """
    write nfo file for series
    :param directory:
    :param title:
    :param rating:
    :param votes:
    :param plot:
    :param id:
    :param genre:
    :param premiered:
    :param studio:
    :param dateadded:
    :return:
    """
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
    title_element = ET2.SubElement(root, 'genre')
    title_element.text = genre
    title_element = ET2.SubElement(root, 'premiered')
    title_element.text = premiered
    title_element = ET2.SubElement(root, 'studio')
    title_element.text = studio
    title_element = ET2.SubElement(root, 'dateadded')
    title_element.text = dateadded
    tree = ET2.ElementTree(root)
    tree.write(os.path.join(directory, 'tvshow.nfo'), pretty_print=True, encoding='UTF-8', xml_declaration=True)
    return


def write_episode_nfo(title, season, episode, plot, airdate, base_link_file):
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
    print '    ' + base_link_file if args.add is not None else None
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
    tree = ET2.ElementTree(root)
    tree.write(base_link_file + '.nfo', pretty_print=True, encoding='UTF-8', xml_declaration=True)
    return


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
    print '    adding new series from ttvdb: ' + title
    ttvdb_base = 'http://www.thetvdb.com/'
    series_id = get_series_id(inetref)
    series_zip_file = os.path.join(config.ttvdb_zips_dir, title_safe + '_' + series_id + '.zip')
    if not os.path.exists(series_zip_file):
        # zip does not exist, download it
        print '    downloading ttvdb zip file...'
        ttvdb_zip_file = ttvdb_base + 'api/' + config.ttvdb_key + '/series/' + series_id + '/all/en.zip'
        urllib.urlretrieve(ttvdb_zip_file, series_zip_file)
        print '        wrote ttvdb zip file: ' + ttvdb_zip_file

    # extract poster, banner, and fanart urls
    print '    ttvdb zip exists, reading xml contents...'
    z = zipfile.ZipFile(series_zip_file, 'r')
    for name in z.namelist():
        if name == 'en.xml':
            z.extract(name, '/tmp/')
            break
    if not os.path.exists('/tmp/en.xml'):
        print '    en.xml not found in series zip file at /tmp/en.xml'
        return False
    else:
        tree = ET.parse('/tmp/en.xml')
        series_data = tree.getroot()
        series = series_data.find('Series')

        rating = series.find('Rating').text
        votes = series.find('RatingCount').text
        plot = series.find('Overview').text
        id = series.find('id').text
        genre = category  # series.find('Genre').text
        premiered = series.find('FirstAired').text
        studio = series.find('Network').text
        dateadded = series.find('added').text

        write_series_nfo(directory, title, rating, votes, plot, id, genre, premiered, studio, dateadded)

        # copy poster, banner, and fanart to link dir
        ttvdb_banners = ttvdb_base + 'banners/'
        poster = series.find('poster').text
        banner = series.find('banner').text
        fanart = series.find('fanart').text
        urllib.urlretrieve(ttvdb_banners + poster, os.path.join(directory, 'poster.jpg'))
        urllib.urlretrieve(ttvdb_banners + banner, os.path.join(directory, 'banner.jpg'))
        urllib.urlretrieve(ttvdb_banners + fanart, os.path.join(directory, 'fanart.jpg'))
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
    print '    adding new series from ttvdb: ' + title
    api_url = 'http://api.themoviedb.org/3'
    headers = {'Accept': 'application/json'}

    request = Request('{url}/configuration?api_key={key}'.format(url=api_url, key=config.tmdb_key), headers=headers)
    cr = json.loads(urlopen(request).read())

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
    request = Request('{url}/movie/{id}?api_key={key}'.format(url=api_url, id=series_id, key=config.tmdb_key))
    mr = json.loads(urlopen(request).read())
    # #### POSTER #####
    poster_path = mr['poster_path']
    poster_url = "{0}{1}{2}".format(base_url, max_poster_size, poster_path)
    # #### BACKDROP #####
    backdrop_path = mr['backdrop_path']
    backdrop_url = "{0}{1}{2}".format(base_url, max_backdrop_size, backdrop_path)

    # print '    poster_path: ' + poster_path
    # print '    backdrop_path: ' + backdrop_path

    request = Request('{url}/movie/{id}/images?api_key={key}'.format(url=api_url, id=series_id, key=config.tmdb_key))
    ir = json.loads(urlopen(request).read())

    # save images
    urllib.urlretrieve(poster_url, os.path.join(directory, 'poster.jpg'))
    urllib.urlretrieve(backdrop_url, os.path.join(directory, 'fanart.jpg'))
    # #### BANNER #####
    # 758 x 140 banner from poster
    img = Image.Image.open(cStringIO.StringIO(urllib.urlopen(poster_url).read()))
    # print 'w: ' + str(img.size[0]) + ' h: ' + str(img.size[1])
    banner_ratio = 758 / float(140)
    # shift crop down by 200 pixels
    box = (0, 220, img.size[0], int(round((img.size[0] / banner_ratio))) + 220)
    img = img.crop(box).resize((758, 140), Image.Image.ANTIALIAS)
    img.save(os.path.join(directory, 'banner.jpg'))

    # print mr
    # print mr['title']
    # print mr['runtime']

    rating = str(mr['vote_average'])
    votes = str(mr['vote_count'])
    plot = str(mr['overview'])
    id = str(mr['id'])
    genre = category
    premiered = str(mr['release_date'])
    studio = ''
    dateadded = ''

    # print 'rating: ' + rating
    # print 'votes: ' + votes
    # print 'plot: ' + plot
    # print 'id: ' + id
    # print 'premiered: ' + premiered

    write_series_nfo(directory, title, rating, votes, plot, id, genre, premiered, studio, dateadded)
    return True

# TODO: handle arguments for file, refresh nfos, etc.
# TODO: clean up symlinks, nfo files, and directories when MythTV recordings are deleted
parser = argparse.ArgumentParser(__file__, description='myth2xbmc... a script for linking XBMC to a MythTV backend.')
parser.add_argument('-a', '--add', dest='add', action='store',
                    help="full path to file name of new recording, used for a MythTV user job upon 'Recording Finished'")
parser.add_argument('-r', '--refresh-nfos', dest='refresh_nfos', action='store_true', default=False,
                    help='refresh all nfo files')
parser.add_argument('-p', '--print-new', dest='print_new', action='store_true', default=False,
                    help='print new recordings not yet linked, mostly used for testing purposes')
# parser.add_argument('--rebuild-library', dest='rebuild_library', action='store_true', default=False,
# help='rebuild library from existing links')
args = parser.parse_args()
# print args.add



def get_recorded_list():
    """
    get recorded list from mythtv database

    :return: xml parsed recorded list
    """
    # print "Looking up from MythTV: " + url + '/Dvr/GetRecordedList'
    tree = ET.parse(urllib.urlopen(BASE_URL + '/Dvr/GetRecordedList'))
    # print Prettify(tree.getroot())
    # quit()
    return tree.getroot()


def check_args():
    if args.print_new is True:
        print 'MYTHTV RECORDINGS NOT LINKED:'

    if args.add is not None:
        print 'ADDING NEW RECORDING: ' + args.add


recorded_list = get_recorded_list()
check_args()


def main():
    """
    main routine

    """
    global source_file
    for program in recorded_list.iter('Program'):

        # print the program for testing
        # print Prettify(program)

        # see if we're adding a new file by comparing the current file name to the argument file name
        base_file_name = get_base_filename_from(program.find('FileName').text)
        if args.add is not None:
            if not base_file_name == get_base_filename_from(args.add):
                continue

        # collect program attributes
        title = program.find('Title').text
        season = program.find('Season').text
        episode = program.find('Episode').text.zfill(2)
        file_extension = program.find('FileName').text[-4:]
        airdate = program.find('Airdate').text
        plot = program.find('Description').text
        inetref = program.find('Inetref').text
        category = program.find('Category').text
        record_date = re.findall('\d*-\d*-\d*', program.find('StartTime').text)[0]

        if inetref is None or inetref == '':
            print 'MISSING INETREF!: ' + title
            # LogMissingInetref()
            continue

        # parse show name for file system safe name
        title_safe = re.sub('[\[\]/\\;><&*:%=+@!#^()|?]', '', title)
        title_safe = re.sub(' +', '_', title_safe)

        # form the file name
        episode_name = title_safe + " - " + season + "x" + episode + " - " + base_file_name
        # if it's a special...
        if season.zfill(2) == "00" and episode == "00":
            episode_name = episode_name + " - " + record_date
            airdate = record_date  # might be needed so specials get sorted with recognized episodes

        # set target link dir
        target_link_dir = os.path.join(config.symlinks_dir, title_safe)
        link_file = os.path.join(target_link_dir, episode_name) + file_extension
        #print 'LINK FILE = ' + link_file

        # skip if link already exists
        if os.path.exists(link_file) or os.path.islink(link_file):
            #print "Symlink " + link_file + " already exists.  Skipping."
            continue

        # find source directory, and if not found, skip it because it's an oprhaned recording!
        source_dir = None
        for mythtv_recording_dir in config.mythtv_recording_dirs[:]:
            source_file = os.path.join(mythtv_recording_dir, base_file_name) + file_extension
            if os.path.isfile(source_file):
                source_dir = mythtv_recording_dir
                break

        if source_dir is None:
            # could not find file!
            print ("Cannot create symlink for " + episode_name + ", no valid source directory.  Skipping.")
            continue

        # this is a new recording, so check if we're just getting a list of them for now
        if args.print_new is True:
            print '   ' + source_file
            continue

        # process new show if found
        if not os.path.exists(target_link_dir):  # and 'Padawan' in titleSafe:
            print "CREATING NEW SERIES... " + title
            os.makedirs(target_link_dir)

            # branch on inetref type
            result = True
            if 'ttvdb' in inetref:
                result = new_series_from_ttvdb(title, title_safe, get_series_id(inetref), category, target_link_dir)
            if 'tmdb' in inetref:
                result = new_series_from_tmdb(title, get_series_id(inetref), category, target_link_dir)

            if result is False:
                print 'ERROR processing images for link_file:'
                print '    ' + link_file
                continue

        # create symlink
        #print "Linking " + source_file + " ==> " + link_file
        os.symlink(source_file, link_file)

        # write the episode nfo file
        write_episode_nfo(title, season, episode, plot, airdate, os.path.splitext(link_file)[0])

        # if adding a new recording, stop looking
        if args.add is not None:
            break

        return


main()
