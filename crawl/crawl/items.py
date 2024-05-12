# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class QuestionItem(scrapy.Item):
    title = scrapy.Field()
    content = scrapy.Field()
    category = scrapy.Field()
    num_answer = scrapy.Field()
    solved = scrapy.Field()
    votes = scrapy.Field()
    views = scrapy.Field()
    time = scrapy.Field()
