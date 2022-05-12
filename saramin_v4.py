import csv
import os.path
import requests
from bs4 import BeautifulSoup
import json
import math
import re
import pandas as pd
from datetime import datetime


def collect_write_numbers():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36"}
    url = "https://www.saramin.co.kr/zf_user/jobs/api/get-search-count?type=unified&cat_kewd=87%2C92%2C84%2C86&company_cd=0%2C1%2C2%2C3%2C4%2C5%2C6%2C7%2C9%2C10&smart_tag="

    response = requests.get(url, headers=headers)
    page_number = math.ceil(json.loads(response.text)['result_cnt'] / 100)
    # page_number = result_cnt if result_cnt % 100 == 0 else result_cnt + 1

    write_numbers = []
    for page in range(1, page_number + 1):
        url = f"https://www.saramin.co.kr/zf_user/search?company_cd=0%2C1%2C2%2C3%2C4%2C5%2C6%2C7%2C9%2C10&cat_kewd=87%2C92%2C84%2C86&recruitPage={page}&recruitSort=relation&recruitPageCount=100&abType=b"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        job_areas = soup.find_all(class_="area_job")
        for job_area in job_areas:
            write_numbers.append(job_area.find("a")['href'].split("?")[1].split("&")[1].split("=")[1])
    return write_numbers


def collect_information(write_numbers):
    write_information_list = []
    company_information_list = []
    benefit_information_list = []

    for write_number in write_numbers:
        url = "https://www.saramin.co.kr/zf_user/jobs/relay/view-ajax"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36"}
        data = {"rec_idx": write_number}
        response = requests.post(url, data=data, headers=headers)
        if response.status_code == 200:
            html = response.text
        soup = BeautifulSoup(html, "html.parser")
        rec_idx = write_number
        rec_title = soup.find(class_="tit_job").text.strip()
        backend_list = ["java", "php", "spring", "node.js",
                        "express", "laravel", "django", "flask", "asp", "jsp", "자바", "스프링", "노드", "익스프레스", "라라벨", "장고",
                        "플라스크", "backend", "back-end", "백엔드", "백앤드"]
        frontend_list = ["javascript", "react", "redux", "vue", "angular", "리액트", "리덕스", "뷰", "앵귤러", "자바스크립트",
                         "frontend", "front-end", "프론트"]
        mobile_list = ["swift", "react native", "앱", "ios", "android", "모바일", "kotlin", "스위프트", "코틀린"]
        etc_list = ["c++", "c#", "mysql", "oracle", "인공지능", "머신러닝", "db"]
        duty_type = ""
        duty_tec = []
        backend_list_filtered = list(filter(lambda x: x in rec_title.lower(), backend_list))
        frontend_list_filtered = list(filter(lambda x: x in rec_title.lower(), frontend_list))
        mobile_list_filtered = list(filter(lambda x: x in rec_title.lower(), mobile_list))
        etc_list_filtered = list(filter(lambda x: x in rec_title.lower(), etc_list))
        if backend_list_filtered:
            if "자바스크립트" in rec_title or "javascript" in rec_title.lower():
                hangul_count = rec_title.count("자바")
                english_count = rec_title.lower().count("java")
                if hangul_count < 2 and english_count < 2:
                    try:
                        backend_list_filtered.remove("자바")
                    except:
                        pass
                    try:
                        backend_list_filtered.remove("java")
                    except:
                        pass
            if backend_list_filtered:
                duty_type = "백엔드"
            backend_list_filtered = list(
                filter(lambda x: x not in ["백엔드", "백앤드", "backend", "back-end"], backend_list_filtered))
            duty_tec.extend(backend_list_filtered)

        if frontend_list_filtered:
            if duty_type == "":
                duty_type = "프론트엔드"
            else:
                duty_type += ",프론트엔드"
            frontend_list_filtered = list(
                filter(lambda x: x not in ["frontend", "front-end", "프론트"], frontend_list_filtered))
            duty_tec.extend(frontend_list_filtered)

        if "풀스택" in rec_title or "full stack" in rec_title.lower():
            if duty_type == "":
                duty_type = "풀스택"
            else:
                duty_type += ",풀스택"

        if mobile_list_filtered:
            if duty_type == "":
                duty_type = "모바일"
            else:
                duty_type += ",모바일"
            mobile_list_filtered = list(filter(lambda x: x not in ["앱", "모바일"], mobile_list_filtered))
            duty_tec.extend(mobile_list_filtered)

        if etc_list_filtered:
            if duty_type == "":
                duty_type = "기타"
            else:
                duty_type += ",기타"
            duty_tec.extend(etc_list_filtered)
        if "웹" in rec_title or "web" in rec_title.lower():
            if "백엔드" not in duty_type and "프론트엔드" not in duty_type and "풀스택" not in duty_type:
                if duty_type == "":
                    duty_type = "웹"
                else:
                    duty_type += ",웹"

        if duty_type == "":
            duty_type = "기타"

        if len(duty_tec):
            duty_tec = ",".join(duty_tec)
        else:
            duty_tec = None
        col1 = soup.find(class_="cont").find(class_="col")
        col2 = soup.find(class_="cont").find_all(class_="col")[1]
        col1_dict = {}
        col1_list = []
        order = ["경력", "학력", "근무형태", "필수사항", "우대사항"]
        dl_list = col1.find_all("dl")
        for dl in dl_list:
            key = dl.find("dt").text.strip()
            if key == "필수사항" or key == "우대사항":
                lis = dl.find("dd").find("ul", class_="toolTipTxt").find_all("li")
                value_list = []
                for li in lis:
                    directive = li.find("span").text.strip()
                    value = li.text.strip().replace(directive, "").replace("\n", "").replace("\r", "")
                    value_list.append(directive + ":" + value)
                value = ",".join(value_list)
            else:
                value = dl.find("dd").find("strong").text.strip()
            col1_dict[key] = value
        for i in order:
            if i in col1_dict:
                col1_list.append(col1_dict[i])
            else:
                col1_list.append(None)
        career = col1_list[0]
        education = col1_list[1]
        work_type = col1_list[2]
        essential = col1_list[3]
        loyalty = col1_list[4]

        order = ["급여", "직급/직책", "근무일시", "근무지역"]
        dl_list = col2.find_all("dl")
        col2_list = convert_dl(order, dl_list)
        pay = col2_list[0]
        position = col2_list[1]
        work_time = col2_list[2]
        work_place = col2_list[3]
        try:
            work_place = soup.find(id="map_0").find("address", class_="address").text.strip().replace("\n", " ")
        except:
            pass
        try:
            d_day = soup.find(class_="dday").text.strip().replace("D-", "")
        except:
            d_day = "접수 마감"
        try:
            company_code = \
                soup.find(class_="jv_cont jv_company").select_one("[csn]")['csn']
        except:
            company_code = None
        write_information = [rec_idx, rec_title, duty_type, duty_tec, career, education, work_type, essential, loyalty,
                             pay, position, work_time, work_place, d_day, company_code]
        print(write_information)
        write_information_list.append(write_information)
        if company_code:
            company_information = collect_company_information(soup)
            print(company_information)
            company_information_list.append(company_information)
        if soup.find(class_="jv_cont jv_benefit"):
            benefit_information = collect_benefit_information(soup, write_number)
            print(benefit_information)
            benefit_information_list.append(benefit_information)

    return [write_information_list, company_information_list, benefit_information_list]


def collect_company_information(soup):
    company_section = soup.find(class_="jv_cont jv_company")
    company_code = company_section.select_one("[csn]")['csn']
    company_name = company_section.find(class_="cont box").find(class_="title").find(class_="company_name").text.strip()
    dl_list = company_section.find(class_="cont box").find(class_="info").find_all("dl")
    order = ["기업형태", "업종", "매출액", "홈페이지", "기업주소", "사원수", "설립일", "대표자명"]
    company_information = [company_code, company_name]
    company_information.extend(convert_dl(order, dl_list, "title"))

    return company_information


def collect_benefit_information(soup, write_number):
    benefit_section = soup.find(class_="jv_cont jv_benefit")
    order = ["지원금/보험", "급여제도", "선물", "교육/생활", "근무 환경", "조직문화", "출퇴근", "리프레시"]
    dl_list = benefit_section.find(class_="cont").find(class_="layer").find_all("dl")
    benefit_information = convert_dl(order, dl_list, type="data-origin")
    benefit_information.insert(0, write_number)

    return benefit_information


def convert_dl(order, dl_list, type=None):
    info_dict = {}
    info_list = []
    for dl in dl_list:
        try:
            key = dl.find("dt").text.strip().replace("*", "")
            if not type:
                value = dl.find("dd").text.strip()
                if key == "급여" and "시간" in value:
                    pat = re.compile(r"\s+상세주.+\s+닫기")
                    value = pat.sub("", value)
            elif type == "data-origin":  # 복리후생일 때
                value = dl.find("dd")[type].strip()
            elif type == "title":  # 기업정보일 때
                value = dl.find("dd")[type].strip().replace("  ", "")

            info_dict[key] = value
        except:
            pass
    for i in order:
        if i in info_dict:
            info_list.append(info_dict[i])
        else:
            info_list.append(None)
    return info_list


def store_csv(information_list):
    write_columns = ["채용글번호", "채용글제목", "직무타입", "직무기술스택", "경력", "학력", "근무형태", "필수사항", "우대사항", "급여", "직급/직책", "근무일시",
                     "근무지역", "남은지원기간", "기업코드"]
    company_columns = ["기업코드", "기업이름", "기업형태", "업종", "매출액", "홈페이지", "기업주소", "사원수", "설립일", "대표자명"]
    benefit_columns = ["채용글번호", "지원금/보험", "급여제도", "선물", "교육/생활", "근무 환경", "조직문화", "출퇴근", "리프레시"]
    columns = [write_columns, company_columns, benefit_columns]
    today = datetime.today().strftime("%Y%m%d%H%M")
    for i, k, j in zip(range(0, 3), ["write", "company", "benefit"], columns):
        with open(today + "_" + k + ".csv", "w", encoding="utf8", newline="") as f:
            csvfile = csv.writer(f)
            csvfile.writerow(j)
            csvfile.writerows(information_list[i])


write_numbers = collect_write_numbers()
print(write_numbers)
information_list = collect_information(write_numbers)
store_csv(information_list)
