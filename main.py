from nltk.corpus import wordnet
from utils.sqlUtils import SQL_Utils
from env import BotVariable
import telepot
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton

class EnglishUtils():
    def __init__(self) -> None:
        from datetime import datetime
        self.AMOUNT_VIDEO_EACH_SEARCH = 10
        self.YOUTUBE_API_KEY = 'AIzaSyBu9JrZUmccASVjpIw4835exe55_Cf9FuI'
        CREATE_TABLE_WORDS = """
        CREATE TABLE IF NOT EXISTS Words (
            word_id INT AUTO_INCREMENT PRIMARY KEY,
            word VARCHAR(255) NOT NULL,
            learn_time INT NOT NULL,
            last_learn DATETIME NOT NULL
        );
        """

        CREATE_TABLE_WORD_MEAN_CONNECTION = """
        CREATE TABLE IF NOT EXISTS WordMeanConnection (
            type VARCHAR(10) NOT NULL,
            mean TEXT NOT NULL,
            word_id INT,
            FOREIGN KEY (word_id) REFERENCES Words(word_id)
        );
        """

        CREATE_TABLE_LEARNING_DASHBOARD = """
        CREATE TABLE IF NOT EXISTS LearningDashboard (
            date DATE NOT NULL,
            review_time INT NOT NULL,
            word_amount INT NOT NULL
        );
        """

        self.SQL = {
            'words': SQL_Utils('Words', CREATE_TABLE_WORDS),
            'wordmean_connection': SQL_Utils('WordMeanConnection', CREATE_TABLE_WORD_MEAN_CONNECTION),
            'learning_dashboard': SQL_Utils('LearningDashboard', CREATE_TABLE_LEARNING_DASHBOARD)
        }
        
        self.languages_code = {
            'English': 'eng',
            'Chinese': 'cmn',
            'Spanish': 'spa',
            'French': 'fra',
            'Portuguese': 'por',
            'Dutch': 'nld',
            'Japanese': 'jpn'
        }

        self.gtts_code = {
            'English': 'en',
            'Chinese': 'zh',
            'Spanish': 'es',
            'French': 'fr',
            'Portuguese': 'pt',
            'Dutch': 'nl',
            'Japanese': 'ja'
        }
        
        self.youglish_code = {
            'English': 'english',
            'Chinese': 'chinese',
            'Spanish': 'spanish',
            'French': 'french',
            'Portuguese': 'portuguese',
            'Dutch': 'dutch',
            'Japanese': 'japanese'
        }

        self.ACCEPT_STATUS = 1
        self.CANCEL_STATUS = 0
        self.BUG_STATUS = -1
        self.INIT_DATETIME = datetime(1111, 11, 11, 0, 0, 0)
        self.FINISH_DATETIME = datetime(9999, 12, 31, 23, 59, 59)

    def __get_type_mean(self, word: str):
        print("GET TYPE MEAN", flush=True)
        synsets = wordnet.synsets(word, lang=self.languages_code[BotVariable.language])
        if synsets:
            result = []
            for _ in synsets:
                if (_.pos() != 's'):
                    result.append([_.pos(), _.definition()])
            return result
        else:
            return []

    def visualize(self, interval: list[str]):
        import plotly.graph_objs as go

        condition = "date BETWEEN '{START_TIME}' AND '{END_TIME}'".format(START_TIME=interval[0], END_TIME=interval[1])
        data = self.SQL['learning_dashboard'].read_record(condition=condition)
        list_review_time = [int(_['REVIEW_TIME']) for _ in data]
        list_datetime = [(_['DATE']).strftime("%Y-%m-%d") for _ in data]
        list_word_amount = [int(_['WORD_AMOUNT']) for _ in data]

        Scatter = go.Scatter(
            x = list_datetime,
            y = list_review_time,
            mode = 'lines+markers',
            name="Reviews",
            yaxis='y2'
        )

        Bar = go.Bar(
            x = list_datetime,
            y = list_word_amount,
            name="Words Learned",
            yaxis='y1'
        )

        layout = go.Layout(
            title = 'Learning Dashboard',
            xaxis = dict(title = 'Date'),
            yaxis1=dict(
                title='Words',
                side='right'  # Position y-axis 1 on the left side
            ),
            yaxis2=dict(
                title="Reviews",
                overlaying='y',
                side='left'
            )
        )

        fig = go.Figure(data=[Scatter, Bar], layout=layout)
        image_bytes = fig.to_image(format="png")
        return image_bytes

    def add_word(self, word: str):
        if (self.SQL['words'].conn.is_connected() == False):
            self.SQL['words'].reset_connect()

        try:
            type_mean = self.__get_type_mean(word)
            word = word.upper()
            if (type_mean == []):
                return self.CANCEL_STATUS

            is_exist = len(self.SQL['words'].read_record(condition="word='{WORD}'".format(WORD=word))) != 0
            if (is_exist == False):
                self.SQL['words'].create_record({'word': word, 'learn_time': 0, 'last_learn': self.INIT_DATETIME})
                word_id = self.SQL['words'].read_record(condition="word = '{WORD}'".format(WORD=word))[0]['WORD_ID']
                for _ in type_mean:
                    [word_type, definition] = _
                    self.SQL['wordmean_connection'].create_record({'type': str(word_type), 'mean': str(definition), 'word_id': word_id})
                return self.ACCEPT_STATUS
            return self.CANCEL_STATUS
        except:
            return self.BUG_STATUS

    def update_dashboard(self):
        from datetime import datetime, timedelta
        if (self.SQL['learning_dashboard'].conn.is_connected() == False):
            self.SQL['learning_dashboard'].reset_connect()

        try:
            CURRENT_DAY = (datetime.now() + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S")
            data = self.SQL['learning_dashboard'].read_record(condition="DATE(date) = DATE(DATE_ADD(NOW(), INTERVAL 7 HOUR)) LIMIT 1")
            is_exist = len(data) != 0
            word_amount = len(self.SQL['words'].read_record(condition="DATE(last_learn) = DATE(DATE_ADD(NOW(), INTERVAL 7 HOUR))"))

            if (is_exist == False):
                self.SQL['learning_dashboard'].create_record({'date': CURRENT_DAY, 'review_time': 1, 'word_amount': word_amount})
            else:
                review_time = data[0]['REVIEW_TIME']
                self.SQL['learning_dashboard'].update_record({'review_time': review_time + 1, 'word_amount': word_amount}, condition="DATE(date) = DATE(DATE_ADD(NOW(), INTERVAL 7 HOUR)) LIMIT 1")

            return self.ACCEPT_STATUS
        except:
            return self.CANCEL_STATUS

class EnglishBot():
    def __init__(self, bot) -> None:
        self.bot = bot
        self.bot_name = 'ENGLISH'
        self.tool = EnglishUtils()

    def __glance(self, msg) -> dict:
        data = msg.upper().split(" ")
        typeMsg = data[0]
        output_command = {'type_msg': typeMsg}

        if (typeMsg == 'CHANGE_BOT'):
            output_command['bot_name'] = data[1]

        elif (typeMsg == 'GET_DATA'):
            pass

        elif (typeMsg == 'REVIEW_DATA'):
            pass

        elif (typeMsg == 'APPEND_DATA'):
            if (len(data) >= 2):
                output_command['command_status'] = data[1]

        return output_command

    def __reset(self):
        BotVariable.chatStates = {}

    def __word_learning(self, chat_id):
        from datetime import datetime, timedelta
        from gtts import gTTS
        from io import BytesIO

        data = self.tool.SQL['words'].read_record(condition="DATE(last_learn) != DATE(DATE_ADD(NOW(), INTERVAL 7 HOUR)) ORDER BY last_learn - INTERVAL learn_time DAY ASC LIMIT 1")
        if (len(data) == 0):
            data = self.tool.SQL['words'].read_record(condition="DATE(last_learn) = DATE(DATE_ADD(NOW(), INTERVAL 7 HOUR)) ORDER BY last_learn ASC LIMIT 1")
        word_id = data[0]['WORD_ID']
        word = data[0]['WORD']
        learn_time = int(data[0]['LEARN_TIME'])
        last_learn = data[0]['LAST_LEARN'].strftime('%Y-%m-%d %H:%M:%S')

        WORD_ID_SEARCHING_QUERY = "word_id='{WORD_ID}'".format(WORD_ID=word_id)
        type_mean_data = self.tool.SQL['wordmean_connection'].read_record(condition=WORD_ID_SEARCHING_QUERY)

        RESPONSE_TEMPLATE = """* STATUS\n  - WORD: {WORD}\n  - LEARN_TIME: {LEARN_TIME}\n  - LAST_LEARN: {LAST_LEARN}\n\n--------------------------------------\n\n* MEANING:\n""".format(WORD=word, LEARN_TIME=learn_time, LAST_LEARN=last_learn)

        for _ in type_mean_data:
            type = _['TYPE']
            mean = _['MEAN']
            RESPONSE_TEMPLATE += "  - ({TYPE}): {MEANING}\n".format(TYPE=type, MEANING=mean)


        YOUGLISH_URL_TEMPLATE = "https://youglish.com/pronounce/{WORD}/{LANGUAGE}?".format(WORD=word, LANGUAGE=self.tool.youglish_code[BotVariable.language])


        RESPONSE_TEMPLATE += "\n TRY TO LISTENING: {URL}".format(URL=YOUGLISH_URL_TEMPLATE)

        KEYBOARD = InlineKeyboardMarkup(inline_keyboard=[
                                            [
                                                InlineKeyboardButton(text="Continue", callback_data="REVIEW_DATA")
                                            ],
                                            [
                                                InlineKeyboardButton(text="Back", callback_data="CHANGE_BOT ENGLISH")
                                            ]
                                        ])

        tts = gTTS(text=word, lang=self.tool.gtts_code[BotVariable.language])

        audio_bytesio = BytesIO()
        tts.write_to_fp(audio_bytesio)
        audio_bytesio.seek(0)

        self.bot.sendVoice(chat_id, audio_bytesio)
        self.bot.sendMessage(chat_id,text=RESPONSE_TEMPLATE, reply_markup=KEYBOARD)

        isLearnedToday = (data[0]['LAST_LEARN']).strftime("%Y-%m-%d") == (datetime.now() + timedelta(hours=7)).strftime("%Y-%m-%d")
        learn_time += (isLearnedToday == False)
        last_learn = (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')
        self.tool.SQL['words'].update_record(newValue={'learn_time': learn_time, 'last_learn': last_learn}, condition=WORD_ID_SEARCHING_QUERY)

    def say_hello(self, chat_id):
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
                                                            [
                                                                InlineKeyboardButton(text='Dashboard', callback_data="GET_DATA")
                                                            ],
                                                            [
                                                                InlineKeyboardButton(text='Learn', callback_data='REVIEW_DATA')
                                                            ],
                                                            [
                                                                InlineKeyboardButton(text='New Word', callback_data='APPEND_DATA'),
                                                            ]
                                                        ])

        self.bot.sendMessage(chat_id, text = "WELCOME TO ENGLISH LEARNING BOT !!! \nClick on buttons to start exploring english learning bot", reply_markup=keyboard)

    def process_msg(self, msg):
        content_type, chat_type, chat_id = telepot.glance(msg)
        if (((chat_id in BotVariable.chatStates) == False) or (BotVariable.chatStates[chat_id] == '')):
            self.say_hello(chat_id)

        else:
            state = self.__glance(BotVariable.chatStates[chat_id])
            if (state['type_msg'] == 'APPEND_DATA'):
                if (state['command_status'] == 'WAITING'):
                    WORD = str(msg['text']).strip().lower()
                    self.__reset()
                    isok = self.tool.add_word(WORD)
                    if (isok == self.tool.ACCEPT_STATUS):
                        self.bot.sendMessage(chat_id, text=str(msg['text']) + " have been added")
                    else:
                        self.bot.sendMessage(chat_id, text=str(msg['text']) + " cannot be added")
                self.say_hello(chat_id)

            elif (state['type_msg'] == 'GET_DATA'):
                from io import BytesIO
                [start_time, end_time] = str(msg['text']).strip().split()
                IMAGE_BYTES = self.tool.visualize([start_time, end_time])
                self.bot.sendPhoto(chat_id, photo=BytesIO(IMAGE_BYTES))

    def process_callback(self, msg):

        query_id, chat_id, query_data = telepot.glance(msg, flavor='callback_query')
        command = self.__glance(query_data)

        if (command['type_msg'] == 'CHANGE_BOT'):
            self.say_hello(chat_id)

        elif (command['type_msg'] == 'REVIEW_DATA'):
            self.__reset()
            self.__word_learning(chat_id)
            self.tool.update_dashboard()

        elif (command['type_msg'] == 'APPEND_DATA'):
            self.__reset()
            BotVariable.chatStates[chat_id] = 'APPEND_DATA WAITING'
            self.bot.sendMessage(chat_id, text="Type your word you want to learn: ")

        elif (command['type_msg'] == 'GET_DATA'):
            self.__reset()
            BotVariable.chatStates[chat_id] = 'GET_DATA'
            self.bot.sendMessage(chat_id, text="Type an interval (YYYY-MM-DD YYYY-MM-DD)")
