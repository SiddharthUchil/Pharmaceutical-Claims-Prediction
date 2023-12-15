import express from 'express'
import * as dotenv from 'dotenv'
import cors from 'cors'
import { Configuration, OpenAIApi } from 'openai'
import { DirectoryLoader } from 'langchain/document_loaders/fs/directory'
import { TextLoader } from "langchain/document_loaders/fs/text"
import { CharacterTextSplitter } from "langchain/text_splitter"
import { OpenAIEmbeddings } from "langchain/embeddings/openai"
import { FaissStore } from "langchain/vectorstores/faiss"
import { ConversationChain } from "langchain/chains";
import { ChatOpenAI } from "langchain/chat_models/openai"
import {
  ChatPromptTemplate,
  HumanMessagePromptTemplate,
  SystemMessagePromptTemplate,
  MessagesPlaceholder,
} from "langchain/prompts"
import { BufferMemory, ChatMessageHistory } from "langchain/memory"
import { PromptTemplate } from "langchain/prompts";
dotenv.config()

const loader = new DirectoryLoader(
  "../FAQ",
  {
    ".txt": (path) => new TextLoader(path)
  }
)

const docs = await loader.load()
const text_splitter = new CharacterTextSplitter({
  chunkSize: 1000,
  chunkOverlap: 0,
});
const documents = await text_splitter.splitDocuments(docs)
const embeddings = new OpenAIEmbeddings()

const vectorstore = await FaissStore.load("./", embeddings)

// await vectorstore.save("./")

const chat = new ChatOpenAI({ temperature: 0 })

const chatPrompt = ChatPromptTemplate.fromPromptMessages([
  SystemMessagePromptTemplate.fromTemplate(
`As a discord FAQ bot for Scotia bank, you have the following information about the bank.

If the customer gives your their name remember it.

If they make an appointment ask them their name, contact number, date and time if not already provided and remember those details

If a question outside this scope is asked, kindly redirect the conversation back to the banking context.

Here are some examples of questions and how you should answer them:

Customer Inquiry: "I am Sid and my contact number is 6474683366. I would like to book an appointment for 1st, August at 9:30 AM:
Your Response: "Sure Sid, your appointment is booked for 1st August at 9:30 AM. Thank you for banking with us"

Customer Inquiry: "What are your operating hours?"
Your Response: "Our bank is open from 9:30 a.m. to 5:00 p.m. from Monday to Friday and on Saturday the closing hours are 4:00 pm. On Sundays, we are closed"

Customer Inquiry: "Are online and mobile banking services secure?"
Your Response: "Yes, Keeping your financial and personal information secure is one of our most important responsibilities. We protect your account using firewalls, 128-bit encryption, 2-step verification, and our digital banking security guarantee."

Customer Inquiry:
Your Response:`),
  new MessagesPlaceholder("history"),
  HumanMessagePromptTemplate.fromTemplate("{input}"),
])


const chain = new ConversationChain({
  memory: new BufferMemory({ returnMessages: true, memoryKey: "history"}),
  prompt: chatPrompt,
  llm: chat,
})


const configuration = new Configuration({
  apiKey: process.env.OPENAI_API_KEY,
});

const openai = new OpenAIApi(configuration)


const app = express()
app.use(cors())
app.use(express.json())

app.get('/', async (req, res) => {
  res.status(200).send({
    message: 'Hello from ScotiaBot!'
  })
})


app.post('/', async (req, res) => {
  try {
    const prompt = req.body.prompt;
    const response = await chain.call({
      input: `${prompt}`,
    })
    console.log({response})
    res.status(200).send({
      bot: response["response"]
    });

  } catch (error) {
    console.error(error)
    res.status(500).send(error || 'Something went wrong');
  }
})

app.listen(5000, () => console.log('AI server started on http://localhost:5000'))