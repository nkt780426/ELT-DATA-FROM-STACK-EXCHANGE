import os
import json
import time
import scrapy
from urllib.parse import urlparse
from crawl.items import QuestionItem
from pykafka import KafkaClient

page = {"start": 1, "end": 10000}
crawl_url = "https://superuser.com/questions?tab=newest&pagesize=50"

kafka_topic_name = "stack_exchange"
kafka_bootstrap_servers = "localhost:39092"


class Spider(scrapy.Spider):
    name = os.path.splitext(os.path.basename(__file__))[0]
    allowed_domains = [urlparse(crawl_url).hostname]
    start_urls = [
        f"{crawl_url}&page={i}" for i in range(page["start"], page["end"] + 1)
    ]

    def __init__(self, *args, **kwargs):
        super(Spider, self).__init__(*args, **kwargs)
        self.kafka_client = KafkaClient(hosts=kafka_bootstrap_servers)
        self.kafka_topic = self.kafka_client.topics[kafka_topic_name]
        self.producer = self.kafka_topic.get_producer()

        # Cấu hình buffer và timeout cho Kafka producer
        # self.producer = self.kafka_topic.get_producer(
        #     linger_ms=10,  # Thời gian chờ tối đa (ms) trước khi gửi một batch
        #     batch_size=16384,  # Kích thước tối đa của mỗi batch (byte)
        #     delivery_reports=True,  # Bật chế độ xác nhận từ Kafka server
        #     request_timeout_ms=5000,  # Thời gian chờ tối đa cho mỗi yêu cầu gửi tin nhắn (ms)
        # )

    def closed(self, reason):
        self.producer.stop()

    def publish_to_kafka(self, item):
        try:
            self.producer.produce(json.dumps(item).encode("utf-8"))
        except Exception as e:
            print(f"Error publishing to Kafka: {e}")
            self.reconnect_to_kafka()

    def reconnect_to_kafka(self):
        max_retries = 3
        current_retry = 0

        while current_retry < max_retries:
            try:
                print("Attempting to reconnect to Kafka...")
                self.kafka_client = KafkaClient(hosts=kafka_bootstrap_servers)
                self.kafka_topic = self.kafka_client.topics[kafka_topic_name]
                self.producer = self.kafka_topic.get_producer()
                print("Reconnected to Kafka successfully.")
                return
            except Exception as e:
                print(f"Error reconnecting to Kafka: {e}")
                current_retry += 1
                time.sleep(2**current_retry)  # Exponential backoff

        print("Max retries reached. Unable to reconnect to Kafka.")

    def parse(self, response):
        # Request tới từng question dựa vào href
        for question in response.css(".js-post-summary"):
            question_url = question.css(
                "div.s-post-summary--content > h3 > a ::attr(href)"
            ).extract_first()
            if question_url.startswith("/questions"):
                item = QuestionItem()

                item["views"] = question.css(
                    ".js-post-summary-stats > div:nth-child(3) > span.s-post-summary--stats-item-number ::text"
                ).extract_first()

                item["num_answer"] = question.css(
                    ".js-post-summary-stats > div:nth-child(2) > span.s-post-summary--stats-item-number ::text"
                ).extract_first()

                item["votes"] = question.css(
                    ".js-post-summary-stats > div.s-post-summary--stats-item.s-post-summary--stats-item__emphasized > span.s-post-summary--stats-item-number ::text"
                ).extract_first()

                item["solved"] = (
                    question.css(
                        ".js-post-summary-stats > div.s-post-summary--stats-item.has-accepted-answer"
                    ).get()
                    is not None
                )

                yield scrapy.Request(
                    response.urljoin(question_url),
                    callback=self.parse_data,
                    cb_kwargs={"item": dict(item)},
                )

    def parse_data(self, response, item):
        item["title"] = response.css("#question-header > h1 > a ::text").extract_first()

        item["content"] = response.css(
            "#question > div.post-layout > div.postcell.post-layout--right > div.s-prose.js-post-body"
        ).extract_first()

        item["time"] = response.css(
            "#content > div > div.inner-content.clearfix > div.d-flex.fw-wrap.pb8.mb16.bb.bc-black-200 > div:nth-child(1) > time ::attr(datetime)"
        ).extract_first()

        item["category"] = response.css(
            "#question > div.post-layout > div.postcell.post-layout--right > div.mt24.mb12 > div > div > ul > li > a ::text"
        ).extract()

        self.publish_to_kafka(item)

        yield item
