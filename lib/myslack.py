from slack_webhook import Slack

class mySlack():
    def __init__(self, url):
        self.myslack = Slack(url=url)

    def post_message(self, message):
        self.myslack.post(text=message)
