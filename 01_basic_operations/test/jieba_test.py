import jieba


if __name__ == '__main__':
    content = "小明硕士毕业于中国科学院研究所, 后在清华大学深造"
    
    result = jieba.cut(content, cut_all=True)
    print(list(result))

    result2 = jieba.cut(content, cut_all=False)
    print(list(result2))
    
    result3 = jieba.cut_for_search(content)
    print(",".join(result3))