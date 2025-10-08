function resolveStorage(persistent) {
    return persistent ? window.localStorage : window.sessionStorage;
}

function removeFromBoth(key) {
    window.localStorage.removeItem(key);
    window.sessionStorage.removeItem(key);
}

export function setAuthTicket(key, ticket, persistent) {
    if (!ticket) {
        removeFromBoth(key);
        return;
    }

    const payload = JSON.stringify(ticket);
    const storage = resolveStorage(persistent);
    storage.setItem(key, payload);

    const otherStorage = resolveStorage(!persistent);
    otherStorage.removeItem(key);
}

export function getAuthTicket(key) {
    const rawSession = window.sessionStorage.getItem(key);
    if (rawSession) {
        try {
            return JSON.parse(rawSession);
        } catch {
            window.sessionStorage.removeItem(key);
        }
    }

    const rawLocal = window.localStorage.getItem(key);
    if (rawLocal) {
        try {
            return JSON.parse(rawLocal);
        } catch {
            window.localStorage.removeItem(key);
        }
    }

    return null;
}

export function clearAuthTicket(key) {
    removeFromBoth(key);
}
