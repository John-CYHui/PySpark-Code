import os
import shutil
import random
import time

TEST_DATA_TEMP_DIR = "/home/dev/Code/05_structuredStreaming/data/tmp/"
TEST_DATA_DIR = "/home/dev/Code/05_structuredStreaming/data/tmp/testdata/"

ACTION_DEF = ['login', 'logout', 'purchase']
DISTRICT_DEF = ['fujian', 'beijing', 'shanghai', 'guangzhou']
JSON_LINE_PATTERN = '{{"eventTime": {}, "action": "{}", "district": "{}"}}\n'

def test_setUp():
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)
    os.mkdir(TEST_DATA_DIR)

def test_tearDown():
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)

def write_and_move(filename, data):
    with open(TEST_DATA_TEMP_DIR + filename, 'wt', encoding='utf-8') as f:
        f.write(data)
    
    shutil.move(TEST_DATA_TEMP_DIR + filename, TEST_DATA_DIR + filename)

if __name__ == "__main__":
    test_setUp()
    
    for i in range(1000):
        print(i)
        file_name = 'e-mall-{}.json'.format(i)
    
        content = ''
        rndcount = list(range(100))
        random.shuffle(rndcount)
        
        for _ in rndcount:
            content += JSON_LINE_PATTERN.format(
                str(int(time.time())),
                random.choice(ACTION_DEF),
                random.choice(DISTRICT_DEF))
            
        write_and_move(file_name, content)
        time.sleep(1)
    
    test_tearDown()
        
    