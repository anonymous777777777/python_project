from elasticsearch import Elasticsearch
import csv
import re

file = open('202205112017_write.csv', 'r', encoding="utf8")

csvfile = csv.reader(file)
next(csvfile)
csvfile = list(csvfile)
file.close()

refined_file = []
pat1 = re.compile(r"경력 (\d+)")
pat2 = re.compile(r"\d,\d{3}")
pat3 = re.compile(r"\d{3}")
mapped_stack = {"자바": "java", "스프링": "spring", "노드": "node.js", "익스프레스": "express", "라라벨": "laravel", "장고": "django",
                "플라스크": "flask",
                "리액트": "react", "리덕스": "redux", "뷰": "vue", "앵귤러": "angular", "자바스크립트": "javascript", "스위프트": "swift",
                "코틀린": "kotlin"}
# recIdx(long),recTitle(text)
for row in csvfile:
    refined_rec_idx = int(row[0])
    refined_rec_title = row[1] if row[1] else None
    refined_duty_type = list(map(lambda x: x.strip(), row[2].split(","))) if row[2] else None
    if row[3]:
        duty_tec = list(map(lambda x: x.strip(), row[3].split(",")))
        duty_tec = [i if i not in mapped_stack.keys() else mapped_stack[i] for i in duty_tec]
        refined_duty_tec = list(dict.fromkeys(duty_tec))
    else:
        refined_duty_tec = None
    if row[4]:
        if "무관" in row[4] or "신입" in row[4]:
            refined_career = None
        elif "경력" in row[4]:
            string = pat1.search(row[4])
            if string:
                refined_career = int(string.group(1))
            else:
                refined_career = 1
        else:
            refined_career = None
    else:
        refined_career = None
    refined_education = row[5] if row[5] else None
    refined_work_type = list(map(lambda x: x.strip(), row[6].split(","))) if row[6] else None
    refined_essential = row[7] if row[7] else None
    refined_loyalty = row[8] if row[8] else None
    if row[9]:
        if "내규" in row[9] or "면접" in row[9]:
            refined_pay = None
        elif "연봉" in row[9]:
            if "억" in row[9]:
                refined_pay = 10000
            else:
                string = pat2.search(row[9])
                if string:
                    refined_pay = int(string.group().replace(",", ""))
                else:
                    refined_pay = None
        elif "월급" in row[9]:
            if pat2.search(row[9]):
                refined_pay = int(pat2.search(row[9]).group().replace(",", "")) * 12
            else:
                refined_pay = int(pat3.search(row[9]).group()) * 12
    else:
        refined_pay = None
    refined_position = list(map(lambda x: x.strip(), row[10].split(","))) if row[10] else None
    refined_work_time = row[11] if row[11] else None
    refined_work_place = row[12] if row[12] else None
    try:
        refined_dday = int(row[13].strip())
    except:
        if "채용시 마감" in row[13] or "상시 채용" in row[13]:
            refined_dday = 365
        else:
            refined_dday = 0
    refined_company_code = row[14] if row[14] else None

    refined_row = [refined_rec_idx, refined_rec_title, refined_duty_type, refined_duty_tec, refined_career,
                   refined_education,
                   refined_work_type, refined_essential, refined_loyalty, refined_pay, refined_position,
                   refined_work_time,
                   refined_work_place, refined_dday, refined_company_code]
    refined_file.append(refined_row)

es = Elasticsearch('http://localhost:9200')
print(es.info())
index_name = 'writes'


for row in refined_file:
    doc = {}
    columns = ["recIdx", "recTitle", "dutyType", "dutyTec", "career"
        , "education", "workType", "essential", "loyalty",
               "pay", "position", "workTime", "workPlace", "dday", "companyCode"]
    for i, j in zip(columns, row):
        doc[i] = j
    es.index(index=index_name, doc_type='_doc', body=doc)


#
# file = open("202205112017_write_refined.csv", "w", encoding="utf8", newline="")
#
# csvfile = csv.writer(file)
# csvfile.writerows(refined_file)
# file.close()

# es = Elasticsearch('http://localhost:9200')
# print(es.info())
# index_name = 'writes'
# #
# es.index(index=index_name, doc_type='string', body=doc1)
