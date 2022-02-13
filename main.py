from consumerlive import kafkaConsumer

if __name__ == '__main__':
    c = kafkaConsumer()
    c.listen_to_topic()