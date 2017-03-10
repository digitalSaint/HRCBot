#!/usr/bin/python
import logging
from bs4 import BeautifulSoup
import urllib2
import json
from bson import json_util
from pymongo import MongoClient
import praw
import re
import datetime
import pytz
import dateutil.parser
import dateutil.tz
import sys

#
# Game Thread: Houston Astros (71-63) @ Texas Rangers (81-54) - Sep 3, 2016, 3:05 PM
#
# away = matches.group(1)
# away_record = matches.group(2)
# home = matches.group(3)
# home_record = matches.group(4)
# game_date = matches.group(5)
# gameday_thread_regex = r'([A-Z][a-z]+ [A-Z][a-z]+) \(([0-9]+-[0-9]+)\) @ ((?:[A-Z][a-z]+ ?)+ [A-Z][a-z]+) \(([0-9]+-[0-9]+)\) - ((?:Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov) (?:[0-9]|[12][0-9]|3[01]), (?:[0-9]{4}), (?:[0-9]{1,2}:[0-5][0-9]) (?:AM|PM))'
#
####### This code is total crap and is more or less a proof of concept that still needs a lot of work ########

class HRCBot:
    """ This is a class for the Home Run Call Bot for r/Astros """
    def __init__(self, team_code, subreddit, mongo_host='localhost', mongo_port=27017, db='hrcbot', log='hrcbot.log'):
        self.reddit = praw.Reddit('hrcbot')
        self.team_code = team_code #'houmlb'
        self.subreddit = self.reddit.subreddit(subreddit) #'astros'
        self.client = self.connect(mongo_host, mongo_port)
        self.db = self.get_db(db)
        date_regex = r'((?:Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov) (?:[0-9]|[12][0-9]|3[01]), (?:[0-9]{4}), (?:[0-9]{1,2}:[0-5][0-9]) (?:AM|PM))'
        record_regex = r'\(([0-9]+-[0-9]+)\)'
        team_regex = r'((?:[A-Z.][a-z.]+ ?)+ [A-Z][a-z]+)'
        gameday_regex = ('{team} {record} @ {team} {record} - {game_date}')
        self.gameday_thread_regex = gameday_regex.format(team=team_regex, record=record_regex, game_date=date_regex)
        self.threshold = datetime.timedelta(minutes=10)
        self.delay = datetime.timedelta(seconds=45)
        self.LOG_FILE = log
        logging.basicConfig(filename=self.LOG_FILE, level=logging.DEBUG)


    def getGameDayThread(self, date):
        """ This function returns a list of GameDay Threads for a given date """
        game_date_string = ''
        date = dateutil.parser.parse(date)
        date_string = date.strftime('%b %d, %Y')
        if date_string[4] == '0':
            game_date_string = date_string[0:3] + ' ' + date_string[5:]
        else:
            game_date_string = date_string
        gameday_threads = []
        #for submission in self.subreddit.hot():
        # For testing only
        for submission in self.subreddit.hot(limit=50):
        #for submission in self.subreddit.search('Game Thread: .* %s, .*' % game_date_string, syntax='plain'): #, time_filter='day'):
        #for submission in self.subreddit.search('Game Thread: .* Aug 11, 2016 12:10 PM'):
            matches = re.search(self.gameday_thread_regex, submission.title)
            if matches:
                thread = {}
                home_team = matches.group(3)
                home_record = matches.group(4)
                away_team = matches.group(1)
                away_record = matches.group(2)
                game_date = dateutil.parser.parse(matches.group(5))
                #print submission.title

                if game_date.strftime('%Y%m%d') == date.strftime('%Y%m%d'):
                    title = submission.title
                    author = str(submission.author)
                    submission_id = submission.id
                    thread['title'] = title
                    thread['game_date'] = game_date
                    thread['author'] = author
                    thread['id'] = submission_id
                    thread['home'] = home_team
                    thread['away'] = away_team
                    thread['hrecord'] = home_record
                    thread['arecord'] = away_record
                    gameday_threads.append(thread)
        return gameday_threads


    def getHRCs(self, thread_id, col_name='hrc', subreddit='astros'):
        """ This function returns a list of homerun calls from the GameDay Thread """
        collection = self.db[col_name]
        hrcs = []
        hrc_regex = r'^[Hh][Rr][Cc]:? (?:[A-Za-z]+ ?)+'
        submission = self.reddit.submission(id=thread_id)
        submission.comments.replace_more(limit=0)
        comment_queue = submission.comments[:]

        while comment_queue:
            hrc = {}
            comment = comment_queue.pop(0)
            if re.search(hrc_regex, comment.body):
                hrc['author'] = str(comment.author)
                hrc['body'] = comment.body
                if not comment.edited == False:
                    edited = datetime.datetime.utcfromtimestamp(comment.edited)
                    edited = dateutil.parser.parse(str(edited)).replace(tzinfo=dateutil.tz.tzutc())
                else:
                    edited = comment.edited
                hrc['edited'] = edited
                hrc['_id'] = comment.id
                hrc['thread_id'] = thread_id
                hrc['subreddit'] = subreddit
                timestamp = datetime.datetime.utcfromtimestamp(comment.created_utc)
                timestamp = dateutil.parser.parse(str(timestamp)).replace(tzinfo=dateutil.tz.tzutc())
                hrc['timestamp'] = timestamp
                hrcs.append(hrc)
                if not collection.find_one(hrc['_id']):
                    collection.insert(hrc)
            comment_queue.extend(comment.replies)
        return hrcs


    def checkTime(self, game):
        """ This function returns a time string in a non-standard format. Don't ask."""
        #print '/'.join([game, 'game.xml'])
        try:
            html = urllib2.urlopen('/'.join([game, 'game.xml']))
            soup = BeautifulSoup(html, 'lxml')
            game_time_et = soup.game['game_time_et']
            if int(game_time_et[1:2]) == 1:
                return '%s%s' % (str(int(game_time_et[0:2])+12-1), game_time_et[2:])
            else:
                return '%s%s' % (str(int(game_time_et[0:2])-1), game_time_et[2:])
        except:
            # Game data is unavailable
            return '0:00 PM'


    def pad(self, value):
        """ This function pads values less than 10 """
        if value < 10:
            return '0%s' % value
        return value


    def getHRs(self, date, thread_id=None, col_name='hr'):
        """ This function returns a list of home runs for a give date """
        # http://gd2.mlb.com/components/game/mlb/year_2016/month_09/day_21/gid_2016_09_21_houmlb_oakmlb_1/atv_runScoringPlays.xml
        collection = self.db[col_name]
        homerun_hitter_regex = r'(^(?:[A-Z.] ?[A-Za-z.]+ ?)+)(?: homers)' # Finds proper names
        parsed_date = dateutil.parser.parse(date)
        year = self.pad(parsed_date.year)
        month = self.pad(parsed_date.month)
        day = self.pad(parsed_date.day)
        hour = self.pad(parsed_date.hour)
        minute = self.pad(parsed_date.minute)
        ampm = ""
        events = []
        games = []
        urldir = 'http://gd2.mlb.com/components/game/mlb/year_%s/month_%s/day_%s/' % (year, month, day)
        html = urllib2.urlopen(urldir)
        soup = BeautifulSoup(html, 'html.parser')
        for li in soup.find_all('li'):
            if self.team_code in li.text:
                games.append(urldir + li.text.strip())
        #if games: print games
        if hour > 12:
            hour = hour - 12
            ampm = "PM"
        elif hour == 12:
            ampm = "PM"
        else:
            ampm = "AM"
        for game in games:
            game = game.strip('/')
            time_string = "%s:%s %s" % (hour, minute, ampm)
            if self.checkTime(game) == time_string:
                try:
                    logging.debug('Run Scoring Plays: %s' % '/'.join([game, 'atv_runScoringPlays.xml']))
                    html = urllib2.urlopen('/'.join([game, 'atv_runScoringPlays.xml']))
                    soup = BeautifulSoup(html, 'lxml')
                    for event in soup.find_all('event'):
                        if event.title.text == 'Home Run':
                            hr = {}
                            # replace white space with single spaces... wtf
                            description = re.sub('\s+', ' ', event.description.text).strip()
                            matches = re.search(homerun_hitter_regex, description)
                            homerun_hitter = matches.group(1).strip() #.split()[-1]
                            event_start = dateutil.parser.parse(event.start.text).replace(tzinfo=dateutil.tz.tzutc())
                            event_end = dateutil.parser.parse(event.end.text).replace(tzinfo=dateutil.tz.tzutc())
                            event_date_str = event_start.strftime('%Y%m%d%H%M%S')
                            hr['_id'] = '%s' % (event_date_str)
                            hr['thread_id'] = thread_id
                            hr['homerun_hitter'] = homerun_hitter
                            hr['event_start'] = event_start
                            hr['event_end'] = event_end
                            if not collection.find_one({'_id': hr['_id']}):
                                collection.insert(hr)
                            events.append(hr)
                except:
                    print 'Game data is unavailable for: %s.' % game
                    raise
        return events


    def getNicknames(self, player):
        """ This idea should be ditched as un-maintainable """
        if player == 'Gattis':
            return [player, 'Oso', 'Blanco']
        elif player == 'Altuve':
            return [player, 'Tuve', 'Altruve']
        elif player == 'Bregman':
            return [player, 'Burgertime', 'Bregs', 'Burgerman', 'Bregstone', 'Bregtrain']
        return [player]


    def compareNames(self, nicknames, hrc):
        """ This function compares home run calls with possible nicknames """
        regex = re.compile(r'^[Hh][Rr][Cc]:?.*\b(?:%s)\b.*' % '|'.join(nicknames))
        hrc = hrc.title() #lower()
        matches = re.search(regex, hrc)
        if matches:
            return True
        return False


    def getWinners(self, HRs, HRCs, col_name='winners'):
        """
        This function checks home runs for matching/correct home run calls.
        """
        collection = self.db[col_name]
        for hr in HRs:
            homerun_hitter = hr['homerun_hitter'].split()
            try:
                names = self.getNicknames(homerun_hitter[1])
            except:
                names = self.getNicknames(homerun_hitter[0])

            logging.debug('Home run info:')
            logging.debug('\ttimestamp: %s' % hr['event_start'])
            logging.debug('\thitter: %s' % homerun_hitter)
            logging.debug('\tnicknames: %s' % names)
            for hrc in HRCs:
                if hrc['edited'] == False:
                    timestamp = hrc['timestamp']
                else:
                    timestamp = hrc['edited']
                if self.compareNames(names, hrc['body']):
                    hr_w_tv_delay = hr['event_start'] + self.delay
                    if hr_w_tv_delay > timestamp:
                        if hr_w_tv_delay - timestamp <= self.threshold:
                            winner = hrc
                            this_hr = self.db['hr'].find_one(hr) # note: this should be changed
                            winner['hr_id'] = this_hr['_id']
                            if not collection.find_one({'_id': hrc['_id']}):
                                collection.insert(winner)

                            correct_hrc = collection.find(winner['author']).count()
                            # Look up number of correct calls and add to string
                            winner_str = '%s correctly predicted the %s home run!' % (hrc['author'], homerun_hitter[1])
                            if correct_hrc > 1:
                                winner_str = '%s\n\n%s now has %s correct predictions this season.' % (winner_str, hrc['author'], str(correct_hrc))
                            elif correct_hrc == 1:
                                winner_str = '%s\n\nThis is %s\'s first correct prediction this season.' % (winner_str, hrc['author'])
                            # Print winner string and make a reply to the comment.
                            print winner_str
                            comment = praw.models.Comment(self.reddit, id=hrc['_id'])
                            comment.reply(winner_str)
                            logging.debug('\t\t%s' % winner_str)
                            logging.debug('\t\tHR: %s, %s' % (hr['event_start'], homerun_hitter))
                            logging.debug('\t\tHR with TV Delay: %s, %s' % (hr_w_tv_delay, homerun_hitter))
                            logging.debug('\t\tHRC: %s, %s' % (timestamp, hrc['body']))
                        else:
                            logging.debug('\tThe HRC happened outside the threshold of the event.')
                            logging.debug('\t\tHR: %s, %s' % (hr['event_start'], homerun_hitter))
                            logging.debug('\t\tHR with TV Delay: %s, %s' % (hr_w_tv_delay, homerun_hitter))
                            logging.debug('\t\tHRC: %s, %s, %s' % (timestamp, hrc['body'], hrc['author']))
                    else:
                        logging.debug('\tHRC happened after the home run was hit.')
                        logging.debug('\t\tHR: %s, %s' % (hr['event_start'], homerun_hitter))
                        logging.debug('\t\tHR with TV Delay: %s, %s' % (hr_w_tv_delay, homerun_hitter))
                        logging.debug('\t\tHRC: %s, %s, %s' % (timestamp, hrc['body'], hrc['author']))
                else:
                    logging.debug('\tCould not find a matching name in the HRC for the Homerun event.')
                    logging.debug('\t\tHRC timestamp: %s' % timestamp)
                    logging.debug('\t\tHRC author: %s' % hrc['author'])
                    logging.debug('\t\tHRC post: %s' % hrc['body'])


    def connect(self, host='localhost', port=27017):
        return MongoClient(host, port)


    def get_db(self, db_name=None):
        return self.client[db_name]


    def get_collection(self, col_name=None):
        return self.db[col_name]


    def main(self, subreddit=None):
#        game_date = dateutil.parser.parse('Aug 11, 2016 12:10 PM') #Sep 21, 2016 2:35 PM') #Oct 2, 2016, 2:05 PM')
#        game_date = dateutil.parser.parse('20160921')
        game_date = datetime.datetime.today()
        logging.debug(game_date)
        threads = self.getGameDayThread(game_date.strftime('%Y%m%d')) #'201610021405'
        for thread in threads:
            game_date = thread['game_date']
            #print game_date
            HRs = self.getHRs(game_date.strftime('%Y%m%d%H%M'), thread['id'])
            HRCs = self.getHRCs(thread['id'])
            logging.debug('date: %s' % game_date)
            logging.debug('thread: https://www.reddit.com/r/%s/%s' % (subreddit, thread['id']))
            if HRs:
                logging.debug(HRs)
                if HRCs:
                    logging.debug(HRCs)
                    self.getWinners(HRs, HRCs)
                else:
                    logging.debug('No home run calls this game.')
            else:
                logging.debug('No homers this game.')
                if HRCs:
                    logging.debug(HRCs)
                else:
                    logging.debug('No home run calls this game.')

            #print json.dumps(thread, indent=2)
            #print json.dumps(self.getHRCs(thread['id']), indent=2, default=json_util.default)
            #print self.getHRCs(thread['id'])


if __name__ == '__main__':
    """
    Set values, initialize the class, and call the main method.
    This code sucks and needs to be rewritten, including a MLB
    library for getting game data.
    """

    subreddit = 'astros'
    team_code = 'houmlb'
    bot = HRCBot(team_code, subreddit)
    bot.main(subreddit)
