import os

from langchain.document_loaders import DirectoryLoader, TextLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter, CharacterTextSplitter
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Chroma
import pickle


def create_store():
    loader = DirectoryLoader(
        "./FAQ", glob="**/*.txt", loader_cls=TextLoader, show_progress=True
    )
    docs = loader.load()
    text_splitter = CharacterTextSplitter(
        chunk_size=300,
        chunk_overlap=100,
    )

    documents = text_splitter.split_documents(docs)
    embeddings = OpenAIEmbeddings(openai_api_key=os.environ.get("OPENAI_API_KEY"))

    vectorstore = Chroma.from_documents(documents, embeddings)

    with open("vectorstore.pkl", "wb") as f:
        pickle.dump(vectorstore, f)

def get_vectorstore():
    with open("vectorstore.pkl", "rb") as f:
        vectorstore = pickle.load(f)
    return vectorstore
