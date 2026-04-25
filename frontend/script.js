const messagesEl = document.querySelector("#messages");
const formEl = document.querySelector("#chatForm");
const inputEl = document.querySelector("#promptInput");
const sendButton = document.querySelector("#sendButton");
const clearButton = document.querySelector("#clearButton");
const newChatButton = document.querySelector("#newChatButton");
const menuButton = document.querySelector("#menuButton");

const storageKey = "agent-chat.messages";

const starterMessages = [
  {
    role: "assistant",
    content:
      "Hi, I am your LLM agent. Ask me to summarize data, draft prompts, or reason through a task.",
  },
];

let messages = loadMessages();
let isThinking = false;

renderMessages();
resizeInput();

formEl.addEventListener("submit", async (event) => {
  event.preventDefault();
  const prompt = inputEl.value.trim();

  if (!prompt || isThinking) {
    return;
  }

  addMessage("user", prompt);
  inputEl.value = "";
  resizeInput();
  setThinking(true);

  await wait(650);
  removeTypingMessage();
  addMessage("assistant", createAgentReply(prompt));
  setThinking(false);
});

inputEl.addEventListener("input", resizeInput);

inputEl.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    formEl.requestSubmit();
  }
});

clearButton.addEventListener("click", resetChat);
newChatButton.addEventListener("click", resetChat);
menuButton.addEventListener("click", () => {
  document.body.classList.toggle("sidebar-open");
});

document.addEventListener("click", (event) => {
  if (
    document.body.classList.contains("sidebar-open") &&
    !event.target.closest(".sidebar") &&
    !event.target.closest("#menuButton")
  ) {
    document.body.classList.remove("sidebar-open");
  }
});

function loadMessages() {
  try {
    const stored = JSON.parse(localStorage.getItem(storageKey));
    if (Array.isArray(stored) && stored.length > 0) {
      return stored;
    }
  } catch {
    localStorage.removeItem(storageKey);
  }

  return [...starterMessages];
}

function saveMessages() {
  localStorage.setItem(storageKey, JSON.stringify(messages));
}

function addMessage(role, content, options = {}) {
  messages.push({ role, content, typing: Boolean(options.typing) });
  renderMessages();
  if (!options.typing) {
    saveMessages();
  }
}

function renderMessages() {
  messagesEl.innerHTML = "";

  for (const message of messages) {
    const row = document.createElement("article");
    row.className = `message-row ${message.role}`;

    if (message.role === "assistant") {
      row.append(createAvatar());
    }

    const bubble = document.createElement("div");
    bubble.className = "message";

    if (message.typing) {
      bubble.innerHTML =
        '<div class="typing" aria-label="Agent is typing"><span></span><span></span><span></span></div>';
    } else {
      const paragraph = document.createElement("p");
      paragraph.textContent = message.content;
      bubble.append(paragraph);
    }

    row.append(bubble);
    messagesEl.append(row);
  }

  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function createAvatar() {
  const avatar = document.createElement("div");
  avatar.className = "avatar";
  avatar.textContent = "AI";
  avatar.setAttribute("aria-hidden", "true");
  return avatar;
}

function setThinking(nextValue) {
  isThinking = nextValue;
  sendButton.disabled = nextValue;

  if (nextValue) {
    addMessage("assistant", "", { typing: true });
  }
}

function removeTypingMessage() {
  messages = messages.filter((message) => !message.typing);
  renderMessages();
}

function resetChat() {
  messages = [...starterMessages];
  saveMessages();
  renderMessages();
  document.body.classList.remove("sidebar-open");
  inputEl.focus();
}

function resizeInput() {
  inputEl.style.height = "auto";
  inputEl.style.height = `${Math.min(inputEl.scrollHeight, 180)}px`;
}

function createAgentReply(prompt) {
  const cleanedPrompt = prompt.replace(/\s+/g, " ").trim();
  const lowerPrompt = cleanedPrompt.toLowerCase();

  if (lowerPrompt.includes("hello") || lowerPrompt.includes("hi")) {
    return "Hi. Tell me what you want to work on and I will keep the answer focused.";
  }

  if (lowerPrompt.includes("data") || lowerPrompt.includes("dataset")) {
    return "I can help inspect the dataset, outline matching logic, or turn results into a concise explanation. Connect this UI to your backend endpoint and I can stream real responses here.";
  }

  if (lowerPrompt.includes("prompt")) {
    return "A good agent prompt should define the role, the available context, the output format, and the constraints. Share the task and I can shape it into a reusable prompt.";
  }

  return `I received: "${cleanedPrompt}"\n\nThis frontend is ready for a real LLM endpoint. Replace createAgentReply() with a fetch call to your agent API when the backend route exists.`;
}

function wait(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}
