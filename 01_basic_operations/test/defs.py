import jieba

def context_jieba(data):
    seg = jieba.cut_for_search(data)
    return list(seg)

def filter_words(data):
    return data not in ["谷", "帮", "客"]

def append_words(data):
    if data == "传智播": data = "传智播客"
    if data == "院校": data = "院校帮"
    if data == "博学": data = "博学谷"
    
    return (data, 1)

def extract_user_and_word(data):
    user_id = data[0]
    content = data[1]
    words = context_jieba(content)
    return_ls = []
    for word in words:
        if filter_words(word):
            return_ls.append((user_id + '_' + append_words(word)[0], 1))
    return return_ls