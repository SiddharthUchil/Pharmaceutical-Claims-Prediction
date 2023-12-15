import express from 'express'
import * as dotenv from 'dotenv'
import cors from 'cors'
import { OpenAI } from 'openai';
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


const chat = new ChatOpenAI({ temperature: 0 })

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

const chatPrompt = ChatPromptTemplate.fromPromptMessages([
  SystemMessagePromptTemplate.fromTemplate(
`As a Sun Life FAQ bot you have the following information about the financial and insurance company.

If someone interacts with you always acknowlege back and remember their name.

Here are the quetions and answers you need to remember. You can paraphrase the answers but always stay in context of answers given to you

Q. What is the project overview?
A. This project introduces a sophisticated multi-label machine learning model designed to predict two critical labels 
associated with drug claims: Label1 and Label2. These labels represent specific drug categories tied to medical diagnoses, 
reflecting the nuanced demands of drug prescription patterns.

Q. What is the project objective?
A. Our objective is to leverage advanced analytics to predict the occurrence of these labels at an individual member level. 
By doing so, we aim to preemptively identify claim patterns and tailor insurance coverage accordingly, while also informing 
inventory management strategies for partnered pharmacies.

Q. What are the common evaluation metrics for multi-label classification model
A. We opted for relevant and informative evaluation metrics suited for multi-label classification tasks. 
These include Hamming Loss, F1 Score, and ROC AUC scores, providing us a comprehensive assessment of our model’s 
predictive capabilities.

Q. What is the dataset all about?
A.The dataset consists of anonymized drug claims data with the following key attributes:
Label1 and Label2: Target variables for multi-label classification.
DINLevel1ClassCode: A classification code potentially predictive of the labels.
ExpenseType: The category of the claim.
ReceivedDate, PaymentIssueDate, ServiceDate: Time-series data relevant for trend analysis.
MemberIDscrambled: Unique identifier for members.
ClaimSubmissionChannel: The channel through which the claim was submitted.
ClaimantAge, ClaimantGender: Demographic information.
FacilityIDscrambled: Unique identifier for the service facility.
MemberCity, MemberProvince: Geographic data for the member.
SubmittedAmount: The amount submitted for the claim.
UniqueClaimCount: The number of unique claims per member.

Q. What are the observations that you made during the EDA process?
A. Upon examining the histograms, we observe that the 'ClaimantAge' feature exhibits a normal distribution, 
indicating a symmetrical spread of claimant ages around the mean. In contrast, the 'SubmittedAmount' feature shows a 
right-skewed distribution, suggesting that while most claims are of lower value, there are a significant number of higher 
value claims as well. This skewness is indicative of outliers or extreme values in the claim amounts.
The box plots reveal that the 'ClaimantAge' feature has a median around 60 years, with a fairly even spread as indicated by the IQR. 
However, the 'SubmittedAmount' feature's box plot shows a median significantly lower than the mean, due to the right-skewed 
distribution observed in the histogram. The whiskers extend far beyond the upper quartile, highlighting the presence of 
outliers in the claim amounts.

Q. Which field/feature is a unique identifier in the dataset?
A. The MemberIDscrambled field is a unique identifier for each member in the dataset.

Q. Should MemberIDScrambled field be used as a feature?
A. MemberIDscrambled field itself should not be used as a direct feature, it can be valuable for feature engineering. 
By aggregating data or calculating statistics based on the MemberIDscrambled, we can create new features that capture meaningful 
patterns or trends in a member's behavior or history.These derived features can provide the model with useful information for 
making predictions while avoiding the pitfalls mentioned above.

If a question outside this scope is asked, kindly redirect the conversation back to the above context always.

`),
  new MessagesPlaceholder("history"),
  HumanMessagePromptTemplate.fromTemplate("{input}"),
])


const chain = new ConversationChain({
  memory: new BufferMemory({ returnMessages: true, memoryKey: "history"}),
  prompt: chatPrompt,
  llm: chat,
})






const app = express()
app.use(cors())
app.use(express.json())

app.get('/', async (req, res) => {
  res.status(200).send({
    message: 'Hello from Sun Life!'
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