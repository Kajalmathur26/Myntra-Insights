# In[1]:


import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
from requests_html import HTMLSession
from selenium import webdriver
import os
from selenium.webdriver.common.action_chains import ActionChains 
from selenium.webdriver.common.keys import Keys


# In[2]:


def get_Myntra_data(soup,p):
    Link=[]
    Uid=[]
    Brand=[]
    Short_name=[]
    Full_name=[]
    Size=[]
    SP=[]
    MRP=[]
    Discount=[]
    Ratings=[]
    Customer_rated=[]
    Page=[]
    Rank=[]

    rank=0
    div=soup.find("div",attrs={"class":"search-searchProductsContainer"})
    litag=div.find_all("li",attrs={"class":"product-base"})
    #print(len(litag))
    for tag in litag:
        rank+=1
        link=tag.find("a").get("href")
        uid=link.split("/")[-2]
        full_name=link.split("/")[2]
        brand=tag.find("div",attrs={"class":"product-productMetaInfo"}).find("h3",attrs={"class":"product-brand"}).text
        short_name=tag.find("div",attrs={"class":"product-productMetaInfo"}).find("h4",attrs={"class":"product-product"}).text
        size=tag.find("div",attrs={"class":"product-productMetaInfo"}).find("h4",attrs={"class":"product-sizes"}).text
        try:
            sp=tag.find("span",attrs={"class":"product-discountedPrice"}).text
        except Exception:
            try:
                sp=tag.find("div",attrs={"class":"product-price"}).text
            except Exception:
                sp=""
        try:
            mrp=tag.find("span",attrs={"class":"product-strike"}).text
        except Exception:
            mrp=""
        try:
            dis=tag.find("span",attrs={"class":"product-discountPercentage"}).text
        except Exception:
            dis=""
        try:
            rating=tag.find("div",attrs={"class":"product-ratingsContainer"}).find("span").text
        except Exception:
            rating=""
        try:
            cr=tag.find("div",attrs={"class":"product-ratingsCount"}).text.replace("|","")
        except Exception:
            cr=""

        Link.append(link)
        Uid.append(uid)
        Brand.append(brand)
        Short_name.append(short_name)
        Full_name.append(full_name)
        Size.append(size)
        SP.append(sp)
        MRP.append(mrp)
        Discount.append(dis)
        Ratings.append(rating)
        Customer_rated.append(cr)
        Page.append(p)
        Rank.append(rank)
    data=list(zip(Link,Uid,Brand,Short_name,Full_name,Size,SP,MRP,Discount,Ratings,Customer_rated,Page,Rank))
    return data


# In[3]:


def get_html(cat,p):
    
    url=f"https://www.myntra.com/{cat}?rawQuery={cat}&p={p}"
    driver=webdriver.Chrome()
    driver.get(url)
    num_scrolls=4
    actions = ActionChains(driver)
    for _ in range(num_scrolls):
        actions.send_keys(Keys.PAGE_DOWN).perform()
        time.sleep(2)
    html=driver.page_source
    driver.quit()
    
    return html


# In[4]:


def get_Myntra(cat,n,full_path):
    data=[]
    error_list=[]
    for p in range(1,n+1):
        
        try:
            html=get_html(cat,p)
        except Exception as e:
            #print("Html_Error",e)
            z=f"{e} error comes while requesting page for {cat} in {p}"
            error_list.append(z)
            continue
        soup=BeautifulSoup(html,"html.parser")
        
        #html_file=f"Myntra_{cat}_{p}.html"
        #html_path=os.path.join(full_path,html_file)
        #with open(html_path,"w",encoding="utf-8") as file:
         #   file.write(str(soup))

        try:
            d=get_Myntra_data(soup,p)
        except Exception as e:
            #print("Data error",cat,"And",p)
            z=f"{e} error comes while extracting data for {cat} in {p}"
            error_list.append(z)
            continue
        
        data.extend(d)
        
        time.sleep(2)
        
    myntra_df=pd.DataFrame(data,columns=["Link","Uid","Brand","Short_name","Full_name","Size","SP","MRP","Discount","Ratings","Customer_rated","Page","Rank"])    
    return [myntra_df,error_list]    
        


# In[5]:

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def configure_session_with_retries():
    # Define the retry strategy
    retry_strategy = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=0.1,
        allowed_methods={"POST"}  # Use allowed_methods and ensure it's a set
    )

    # Create an adapter with the retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)

    # Create a session and mount the adapter
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session



# In[7]:


def Myntra(category,n):
    ctime=datetime.now()
    ctime=ctime.strftime("%d-%m-%y %H_%M_%S")
    folder=f"Myntra_{ctime}"
    path=r"C:\Users\Admin\Downloads\myntra K\Myntra Data -2"
    full_path=os.path.join(path,folder)
    os.makedirs(full_path)
    main_folder="Myntra  Data"
    list_df=[]
    list_file=[]
    error=[]
    for cat in category:
        res=get_Myntra(cat,n,full_path)
        #time.sleep(2)
        excel_path=os.path.join(full_path,f"Myntra_{cat}.xlsx")
        res[0].to_excel(excel_path,index=False)
        list_df.append(res[0])
        
        list_file.append(f"Myntra_{cat}.xlsx")
        error.extend(res[1])

        
    return print("Done")


# In[ ]:
HEADERS=({"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0","Accept-Language":"en-US, en;q=0.5"})
category=["Facewash" ,"Foundation","Lipstick","Kajal","Mascara","Blushes","Shampoo","Hair Oil","Conditioner","Face Serum","Moisturizers","Sunscreen" , "Smart watch" , "Headphone"]
page=4
print(Myntra(category,page))





