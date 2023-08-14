## Python Task
##### Create environment
- Linux, macOS: `python3 -m venv .venv/`
- Windows: `python -m venv .venv/`
##### Activate environment
- Linux, macOS: `source .env/bin/activate`
- Windows: `.env\Scripts\activate`
##### Command to install packages
- `pip install -r requirements.txt`
#### Download the Dataset
- to download the dataset, please go to this link "https://www.kaggle.com/datasets/Cornell-University/arxiv", download it. Then, extract it to the same folder of the project (since the file was too large to commit here)
#### COMMAND TO RUN THE PROJECT
uvicorn main:app --reload 
