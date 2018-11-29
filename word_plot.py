# from wordcloud import WordCloud, STOPWORDS
import csv
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt

path_positive = 'results\\adjectives_frequency\\postivei\\words.csv'
path_negative = 'results\\adjectives_frequency\\negative\\words.csv'
path_neutral = 'results\\adjectives_frequency\\neutral\\words.csv'

stopwords = set(STOPWORDS)
stopwords.add('first')
stopwords.add('second')
stopwords.add('sure')
stopwords.add('many')
stopwords.add('last')
stopwords.add('old')
stopwords.add('new')
stopwords.add('overall')
stopwords.add('>')
stopwords.add('<')
stopwords.add('able')
stopwords.add('available')
stopwords.add('br')
stopwords.add('various')
stopwords.add('kindle')
stopwords.add('next')
stopwords.add('previous')

def saveAndCreatePlots(path, destination):
    csv_ = csv.reader(open(path, 'r', newline='\n', encoding='utf-8'))
    dict = {}

    for elements in csv_:
        if elements[0] not in stopwords:
            dict[elements[0]] = float(elements[1])

    wc = WordCloud(background_color='white').generate_from_frequencies(dict)

    fig = plt.figure(figsize=(20,10) )
    plt.imshow(wc, interpolation='bilinear')
    plt.axis("off")
    plt.savefig(destination, dpi=fig.dpi)

saveAndCreatePlots(path_positive, 'figures\\positive_adjectives.png')
saveAndCreatePlots(path_negative, 'figures\\negative_adjectives.png')
saveAndCreatePlots(path_neutral, 'figures\\neutral_adjectives.png')