import csv
import multiprocessing
import multiprocessing as mp

class TF_IDF:
    def __init__(self):
        self.words = {}
        self.tag_words = {}
        self.punctuation = ",.:?!'\""
        self.translation_table = (",.:?!'<>()\"", '           ')

    def split_data(self, tags, text):
        text = text.translate(self.translation_table).split()
        tags = tags.split()
        return tags, text

    def add_tagged_text(self, text, tags):
        text = text.translate(self.translation_table).split()
        tags = tags.split()

        for tag in tags:
            if tag not in self.tag_words:
                self.tag_words[tag] = {}

            for word in text:
                # if word in self.tag_words[tag]:
                #     self.tag_words[tag][word] += 1
                # else:
                #     self.tag_words[tag][word] = 1
                self.tag_words[tag][word] += 1

        for word in self.tag_words[tags[0]]:
            # if word in self.words:
            #     self.words[word] += 1
            # else:
            #     self.words[word] = 1
            self.words[word] += 1

base = TF_IDF();

print( "число процессоров (ядер) = {0:d}".format( multiprocessing.cpu_count() ) )

with open('/home/vladimir/Train/TrainLight.csv', 'r') as train:
            csv_reader = csv.reader(train, delimiter=',')
            for row in csv_reader:
                base.add_tagged_text(text=row[0], tags=row[1])


def emit(q):
       with open('/home/vladimir/Train/TrainLight.csv', 'r') as train:
        csv_reader = csv.reader(train, delimiter=',')
        for row in csv_reader:
            q.put(row)

def handle(q):
    while not q.empty():
        message = q.get()
        base.add_tagged_text(message[2], message[3])


class MP_CSV_PARSER:
    def __init__(self, parse_function, count_of_processes=4):
        self.jobs = mp.Queue()
        self.results = mp.Queue()
        self.parsed_results =
        self.count_of_processes = count_of_processes
        self.parse_function = parse_function
        self.done = False

    def create_processes(self):
        for i in range(self.count_of_processes):
            process = mp.Process(target=self.worker, args=(i,))
            process.daemon = True
            process.start()
            # process.join()

    def worker(self, num):
        while True:
            job = self.jobs.get()
            # print(job)
            try:
                #self.parse_function(job[0], job[1])
                base.add_tagged_text(text=job[0], tags=job[1])
                # base.add_tagged_text('test', 'test')
            except TypeError:
                print('error')
            except IndexError:
                print('error')

    def emitter(self):
        with open('/home/vladimir/Train/TrainLight.csv', 'r') as train:
            csv_reader = csv.reader(train, delimiter=',')
            for row in csv_reader:
                self.jobs.put(row)

            self.done = True

    def merger(self):


    def start_parsing(self):
        self.create_processes()
        self.emitter()
        # self.jobs.join_thread()

pars = MP_CSV_PARSER(parse_function=None, count_of_processes=8)
pars.start_parsing()




