#Below code is written and tested on Python 2.7
import tweepy
import sys
import pydocumentdb.documents as documents
import pydocumentdb.document_client as document_client
import pydocumentdb.errors as errors

class IDisposable:
    """ A context manager to automatically close an object with a close method
    in a with statement. """
    def __init__(self, obj):
        self.obj = obj
    def __enter__(self):
        return self.obj # bound to target
    def __exit__(self, exception_type, exception_val, trace):
        # extra cleanup in here
        self = None
credentials = None

def insertintoCosmosDB(cdbhost, cdbmasterkey, tweetDate, tweetText):
    tweetmessage = {'tweetDate': str(tweetDate),'id' : str(tweetDate), 'tweetText': tweetText}
    _database_link = 'dbs/tweetdb'
    _collection_link = _database_link + '/colls/tweetcollec'
    with IDisposable(document_client.DocumentClient(cdbhost, {'masterKey': cdbmasterkey} )) as client:
        try:
            client.CreateDocument(_collection_link, tweetmessage, options=False)
        except errors.DocumentDBError as e:
            if e.status_code == 409:
                pass
            else:
                raise errors.HTTPFailure(e.status_code)

def main():
 # Twitter application key
    _appkey = "ffmaQW7NcFoJj9IjRTIfLAwxNww" # KeyVault URL, Secret, Version
    _appsecret= "nnO0ntgRXoM8qxLkQg10IW3TwOqUCRlAC3cKPkn1Gi18ABekGouu" # KeyVault URL, Secret, Version
    _appaccesstoken = "11028208415955447808-npzcLoTsIVPp4rIMfAYYayE3c5mbiLl" # KeyVault URL, Secret, Version
    _appaccesstokensecret = "jjtR3VEdH5Y22TzhkP0157G51wPmC4QC4yk2YTMfTyeMxww" # KeyVault URL, Secret, Version

    _tweetTag= sys.argv[1] # like Azure 
    _tweetReadSince=  sys.argv[2] #date from when you want to read tweets like '2018/07/28'
    _RandomId = sys.argv[3] #Azure Data Factory Pipeline ID 'testrun' 
  
# CosmosDB Credential
    #_cdbhost = client.get_secret("https://XXXXXXXXX.vault.azure.net/", "cosmosdbURI", "XXXXXXXXXXXXXXXXXXXXXXXXXX") # KeyVault URL, Secret, Version
    #_cdbmasterkey = client.get_secret("https://XXXXXXXX.vault.azure.net/", "cosmosdbPK", "XXXXXXXXXXXXXXXXXXXXXXXXXX") # KeyVault URL, Secret, Version
    
#hashtag, tweetreadsince, filename includes pipeline id, 
    auth = tweepy.OAuthHandler(_appkey, _appsecret)
    auth.set_access_token(_appaccesstoken, _appaccesstokensecret)
    tweetapi = tweepy.API(auth,wait_on_rate_limit=True)

    for tweet in tweepy.Cursor(tweetapi.search,q=_tweetTag,lang="en", since=_tweetReadSince).items(15):
        try:
            if tweet.text.encode('utf-8') != '' : 
                #insertintoCosmosDB (_cdbhost.value, _cdbmasterkey.value, tweet.created_at,tweet.text.encode('utf-8'))
                print(tweet.text.encode('utf-8'))
        except errors.DocumentDBError as e:
            if e.status_code == 409:
                pass
            else:
                raise errors.HTTPFailure(e.status_code)
                print("Error while fetching and storing tweets!!!")
            break
    
if __name__ == "__main__":
	main()
