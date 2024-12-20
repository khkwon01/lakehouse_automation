# lakehouse_automation
lakehouse_automation

## Overall diagram

![image](https://github.com/user-attachments/assets/280a0bf4-372e-4fc5-beeb-3b86c3fa0a35)


### 1. install s3fs and oci setup on VM
- 설치
  ```
  sh lakehouse_setup.sh
  ```
- 실행화면  
  <img width="1276" alt="image" src="https://github.com/user-attachments/assets/5429794b-bcd6-4cf5-9988-4af305225e6f">

### 2. Temporary data generation and apply incremental files to lakehouse
  #### 1) Temporary data generation
  - execute the below python program
    - python version : 3.9
    - package
      - schedule==1.2.2
      - pytz==2024.2
    ```
    python datagen.py
    ```

  #### 2) Apply incremental files to lakehouse
  - execute the below python program
    - python version : 3.9
    - package
      - mysql-connector-python==9.1.0
      - pandas==2.2.3
      - pytz==2024.2
    ```
    python check_lakehouse.py
    ```

### 3. Result the screen
![image](https://github.com/user-attachments/assets/46fdbf1c-cfea-4908-ae4a-f8b37d6813f6)

