const messagesEl = document.querySelector("#messages");
const formEl = document.querySelector("#chatForm");
const inputEl = document.querySelector("#promptInput");
const latitudeEl = document.querySelector("#latitudeInput");
const longitudeEl = document.querySelector("#longitudeInput");
const sendButton = document.querySelector("#sendButton");
const clearButton = document.querySelector("#clearButton");
const newChatButton = document.querySelector("#newChatButton");
const menuButton = document.querySelector("#menuButton");
const locationGridEl = document.querySelector("#locationGrid");

const storageKey = "hospital-matcher.messages";
let appConfig = { test_mode: false };

const starterMessages = [
  {
    role: "assistant",
    content:
      "Describe the diagnosis or symptoms in English and I will guide the next step.",
  },
];

let messages = loadMessages();
let isThinking = false;

initialize();

formEl.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (isThinking) {
    return;
  }

  const prompt = inputEl.value.trim();

  if (!prompt) {
    return;
  }

  let latitude = null;
  let longitude = null;
  if (appConfig.test_mode) {
    latitude = Number.parseFloat(latitudeEl.value);
    longitude = Number.parseFloat(longitudeEl.value);
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
      addMessage("assistant", "Please enter a valid latitude and longitude.");
      return;
    }
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

async function initialize() {
  appConfig = await fetchAppConfig();
  applyAppMode();
  renderMessages();
  resizeInput();
}

async function fetchAppConfig() {
  try {
    const response = await fetch("/api/app-config");
    if (!response.ok) {
      return { test_mode: false };
    }
    return await response.json();
  } catch {
    return { test_mode: false };
  }
}

function applyAppMode() {
  if (appConfig.test_mode) {
    starterMessages[0].content =
      "Enter a supported diagnosis name plus latitude and longitude, and I will return the best 5 matching hospital options.";
    locationGridEl.hidden = false;
    inputEl.placeholder = "Enter a supported diagnosis, for example: Dentistry";
    return;
  }

  starterMessages[0].content =
    "Describe the diagnosis or symptoms in English. In non-test mode, only text input is exposed.";
  locationGridEl.hidden = true;
  inputEl.placeholder = "Describe the diagnosis or symptoms";
}

async function fetchRecommendation(query, latitude, longitude) {
  let response;
  try {
    response = await fetch("/api/recommend", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        query,
        latitude,
        longitude,
        limit: 5,
      }),
    });
  } catch {
    throw new Error("Backend not reachable. Start the app with: python3 app/server.py");
  }

  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "The backend could not process that request.");
  }
  return payload;
}

function formatUserMessage(prompt, latitude, longitude) {
  if (!appConfig.test_mode) {
    return prompt;
  }
  return `${prompt}\nLatitude: ${latitude}\nLongitude: ${longitude}`;
}

function formatRecommendation(result) {
  if (result.message) {
    return result.message;
  }

  const diagnosis = result.diagnosis?.name || "Unknown";
  const confidence = result.diagnosis?.confidence_score ?? 0;
  const matches = Array.isArray(result.matches) ? result.matches : [];
  const extraction = result.extraction;
  const bestMatch = result.best_match;
  const lines = [];

  if (result.test_mode) {
    lines.push("Test mode is active.");
    lines.push("Using your diagnosis and coordinates directly.");
    lines.push("");
  } else if (extraction) {
    lines.push(`Extracted diagnosis: ${extraction.diagnosis_name}`);
    lines.push(`Extracted location: ${extraction.location_text}`);
    lines.push(
      `Approximate coordinates: ${extraction.latitude.toFixed(4)}, ${extraction.longitude.toFixed(4)}`
    );
    if (extraction.geocoding_used) {
      lines.push(`Coordinates source: ${extraction.geocoding_source || "geocoding API"}`);
    } else {
      lines.push("Coordinates source: LLM estimate");
    }
    lines.push(`Need summary: ${extraction.need_description}`);
    lines.push("");
  }

  lines.push(`Diagnosis category: ${diagnosis}`);
  if (confidence) {
    lines.push(`Confidence: ${confidence}`);
  }
  lines.push("");

  if (matches.length === 0) {
    lines.push("No matching provider was found.");
    return lines.join("\n");
  }

  if (bestMatch) {
    lines.push(`Best match: ${bestMatch.name}`);
    if (bestMatch.selection_reason) {
      lines.push(`Why: ${bestMatch.selection_reason}`);
    }
    lines.push("");
  }

  lines.push("Best 5 options:");
  for (const [index, match] of matches.entries()) {
    const details = [`${index + 1}. ${match.name}`, match.type];
    if (Number.isFinite(match.distance_km)) {
      details.push(`${match.distance_km.toFixed(2)} km`);
    }
    if (Number.isFinite(match.trustworthy_score)) {
      details.push(`trust ${match.trustworthy_score.toFixed(2)}`);
    }
    lines.push(details.join(" | "));
  }

  return lines.join("\n");
}
