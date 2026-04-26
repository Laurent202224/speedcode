const messagesEl = document.querySelector("#messages");
const formEl = document.querySelector("#chatForm");
const inputEl = document.querySelector("#promptInput");
const sendButton = document.querySelector("#sendButton");
const clearButton = document.querySelector("#clearButton");
const newChatButton = document.querySelector("#newChatButton");
const menuButton = document.querySelector("#menuButton");
const agentStatusEl = document.querySelector("#agentStatus");

const storageKey = "hospital-matcher.messages";

const starterMessages = [
  {
    role: "assistant",
    content:
      "Describe the patient's symptoms or care need and include latitude/longitude. I will choose the doctor category and return the 5 closest matching providers.",
  },
];

let messages = loadMessages();
let isThinking = false;

renderMessages();
resizeInput();
loadAppConfig();

formEl.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (isThinking) {
    return;
  }

  const prompt = inputEl.value.trim();

  if (!prompt) {
    addMessage("assistant", "Please describe the patient's symptoms or care need and include a location.");
    return;
  }

  addMessage("user", prompt);
  inputEl.value = "";
  resizeInput();
  setThinking(true);

  try {
    const result = await fetchRecommendation(prompt);
    removeTypingMessage();
    addMessage("assistant", result.message || formatRecommendation(result));
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

async function fetchRecommendation(query) {
  const response = await fetch("/api/recommend", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query,
      limit: 5,
    }),
  });

  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "The backend could not process that request.");
  }
  return payload;
}

async function loadAppConfig() {
  try {
    const response = await fetch("/api/config");
    const payload = await response.json();
    if (payload.llm?.enabled) {
      const selector = payload.hospital_selection_llm?.model || "selector";
      const reviews = payload.google_reviews?.enabled ? "reviews on" : "reviews off";
      agentStatusEl.textContent = payload.llm.model
        ? `LLM: ${payload.llm.model} + ${selector}, ${reviews}`
        : `LLM enabled + ${selector}, ${reviews}`;
    } else {
      agentStatusEl.textContent = "Local matcher";
    }
  } catch {
    agentStatusEl.textContent = "Agent ready";
  }
}

function formatRecommendation(result) {
  if (result.message) {
    return result.message;
  }

  const diagnosis = result.diagnosis?.name || "Unknown";
  const confidence = result.diagnosis?.confidence_score ?? 0;
  const urgency = result.diagnosis?.urgency;
  const source = result.diagnosis?.source;
  const matches = Array.isArray(result.matches) ? result.matches : [];
  const bestMatch = result.hospital;
  const selection = result.hospital_selection || {};

  if (!bestMatch && matches.length === 0) {
    return `Doctor category: ${diagnosis}\nConfidence: ${confidence}\nNo nearby provider in the dataset matched this category.`;
  }

  return [
    result.llm?.enabled && result.llm?.error
      ? `LLM response unavailable; showing local match.\n${result.llm.error}`
      : "",
    `Doctor category: ${diagnosis}`,
    `Confidence: ${confidence}`,
    urgency ? `Urgency: ${urgency}` : "",
    source ? `Selected by: ${source}` : "",
    bestMatch ? formatSelectedHospital(bestMatch, selection) : "",
    "Closest matching providers considered:",
    ...matches.slice(0, 5).map(formatProviderLine),
  ]
    .filter(Boolean)
    .join("\n");
}

function formatSelectedHospital(provider, selection) {
  const distance = Number.isFinite(provider.distance_km)
    ? `${provider.distance_km.toFixed(2)} km`
    : "distance unavailable";

  return [
    `Best hospital: ${provider.name} (${provider.type})`,
    `Distance: ${distance}`,
    selection.treats ? `What they treat: ${selection.treats}` : "",
    selection.location ? `Where it is situated: ${selection.location}` : "",
    selection.reason ? `Why this hospital: ${selection.reason}` : "",
    formatGoogleReviews(provider),
    selection.website ? `Website: ${selection.website}` : "",
    selection.phone ? `Phone: ${selection.phone}` : "",
    selection.error ? `Second LLM issue: ${selection.error}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}

function formatGoogleReviews(provider) {
  const reviews = provider.google_reviews;
  if (!reviews || !reviews.available) {
    return "";
  }

  const parts = [];
  if (reviews.rating !== undefined && reviews.rating !== null) {
    parts.push(`${reviews.rating}/5`);
  }
  if (reviews.user_rating_count !== undefined && reviews.user_rating_count !== null) {
    parts.push(`${reviews.user_rating_count} reviews`);
  }
  if (reviews.google_maps_url) {
    parts.push(reviews.google_maps_url);
  }

  return parts.length ? `Google reviews: ${parts.join(", ")}` : "";
}

function formatProviderLine(provider, index) {
  const distance = Number.isFinite(provider.distance_km)
    ? `${provider.distance_km.toFixed(2)} km`
    : "distance unavailable";
  return `${index + 1}. ${provider.name} (${provider.type}) - ${distance}`;
}
