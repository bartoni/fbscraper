from bs4 import BeautifulSoup
import io
import re
import os
import multiprocessing
from datetime import datetime


def scrape(filename):

    comment_counter = 0
    content_clean = ''
    rows = []
    users = []

    with io.open(' ' % filename, encoding="utf8") as fp:
        soup = BeautifulSoup(fp, features="html.parser")

    comments = soup.find_all('span', {'class' : '_3l3x'})

    content = soup.find('div', {'class' : '_3w8y'})

    for div in soup.findAll('div', {'class':'_72vr'}):
        users.append(str(div.find('a').contents[0]))

    #remove duplicates in usernames list
    users = list(set(users))

    prev_item = ''

    n = 1

    for item in comments:
        comment = item.get_text()
        if item != prev_item:
            query = comment.split()

            #remove usernames from comments
            if ' '.join(query[:3]) in users:
                query = query[3:]
            elif ' '.join(query[:2]) in users:
                    query = query[2:]

            #remove links
            text = re.sub(r'(https?:\/\/|www\.).[^\s]+', '', ' '.join(query))

            #save comments with text only (due to encoding there are some blank comments)
            if not str.isspace(text) and text:
                comment_counter += 1
                rows.append(text)

    #remove links in content
    if content:
        content_clean = re.sub(r'https?:\/\/.[^\s]+', '', content.get_text(separator=' '))

    #save as txt filename with a total number of comments under the post
    filename_txt = filename[:-5] + '_' + str(comment_counter)

    with open(' ' % filename_txt, 'w', encoding="utf8") as f:
        if not str.isspace(content_clean) and content_clean:
            f.write(content_clean + '\n')
        for item in rows:
            f.write("\n%s\n" % item)

    return comment_counter

if __name__ == '__main__':
    #to see how long it takes to process
    startTime = datetime.now()
    results = []
    #path to html files
    basepath = ' '

    with os.scandir(basepath) as entries:
        #to speed it up :)
        pool = multiprocessing.Pool()
        for entry in entries:
            if entry.is_file():
                results.append(pool.apply_async(scrape, args=(entry.name,)))
        pool.close()
        print('Processing...')
        pool.join()
        output = [p.get() for p in results]
        print(sum(output))
        print(datetime.now() - startTime)



#div class="_3w8y" = post content
#div class="_7a9h = response
#<span class="_6qw4"> = OP username
#div class="_72vr" = username in replies
