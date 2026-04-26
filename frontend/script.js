const messagesEl = document.querySelector("#messages");
const formEl = document.querySelector("#chatForm");
const inputEl = document.querySelector("#promptInput");
const latitudeEl = document.querySelector("#latitudeInput");
const longitudeEl = document.querySelector("#longitudeInput");
const sendButton = document.querySelector("#sendButton");
const clearButton = document.querySelector("#clearButton");
const newChatButton = document.querySelector("#newChatButton");
const menuButton = document.querySelector("#menuButton");

const storageKey = "hospital-matcher.messages";

const starterMessages = [
  {
    role: "assistant",
    content:
      "Describe the diagnosis or symptoms in English, enter latitude and longitude, and I will return the nearest hospital or clinic that matches the treatment.",
  },
];

let messages = loadMessages();
let isThinking = false;

renderMessages();
resizeInput();

formEl.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (isThinking) {
    return;
  }

  const prompt = inputEl.value.trim();
  const latitude = Number.parseFloat(latitudeEl.value);
  const longitude = Number.parseFloat(longitudeEl.value);

  if (!prompt) {
    return;
  }

  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    addMessage("assistant", "Please enter a valid latitude and longitude.");
    return;
  }

  addMessage("user", formatUserMessage(prompt, latitude, longitude));
  inputEl.value = "";
  resizeInput();
  setThinking(true);

  try {
    const result = await fetchRecommendation(prompt, latitude, longitude);
    removeTypingMessage();
    addMessage("assistant", formatRecommendation(result));
  } catch (error) {
    removeTypingMessage();
    addMessage("assistant", error.message);
  } finally {
    setThinking(false);
  }
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

async function fetchRecommendation(query, latitude, longitude) {
  const response = await fetch("/api/recommend", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query,
      latitude,
      longitude,
      limit: 3,
    }),
  });

  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "The backend could not process that request.");
  }
  return payload;
}

function formatUserMessage(prompt, latitude, longitude) {
  return `${prompt}\nLocation: ${latitude.toFixed(4)}, ${longitude.toFixed(4)}`;
}

function formatRecommendation(result) {
  const diagnosis = result.diagnosis?.name || "Unknown";
  const confidence = result.diagnosis?.confidence_score ?? 0;
  const bestMatch = result.hospital;

  if (!bestMatch) {
    return `Diagnosis category: ${diagnosis}\nConfidence: ${confidence}\nNo nearby provider in the dataset matched this treatment.`;
  }

  const distance = Number.isFinite(bestMatch.distance_km)
    ? `${bestMatch.distance_km.toFixed(2)} km`
    : "distance unavailable";

  return [
    `Diagnosis category: ${diagnosis}`,
    `Confidence: ${confidence}`,
    `Best match: ${bestMatch.name}`,
    `Type: ${bestMatch.type}`,
    `Distance: ${distance}`,
    bestMatch.description ? `Description: ${bestMatch.description}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}
