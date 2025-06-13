export const delayMs = (ms) => new Promise(resolve => setTimeout(resolve, ms));

export function parseHumanReadableSize(str) {
    const sizeInput = str.trim().toUpperCase();
    if (sizeInput.endsWith('KB')) {
        return parseFloat(sizeInput) * 1024;
    } else if (sizeInput.endsWith('MB')) {
        return parseFloat(sizeInput) * 1024 * 1024;
    } else if (sizeInput.endsWith('GB')) {
        return parseFloat(sizeInput) * 1024 * 1024 * 1024;
    } else if (sizeInput.endsWith('TB')) {
        return parseFloat(sizeInput) * 1024 * 1024 * 1024 * 1024;
    } else if (sizeInput.endsWith('PB')) {
        return parseFloat(sizeInput) * 1024 * 1024 * 1024 * 1024 * 1024;
    } else {
        return parseFloat(sizeInput);
    }
}

export async function fetchText(uri) {
    try {
        const response = await fetch(uri);
        if (!response.ok) {
            throw { message: `HTTP error! Status: ${response.status}` };
        }
        const text = await response.text();
        return text;
    } catch (error) {
        console.error(`Failed to fetch from URI: ${uri}`, error);
        throw error;
    }
}
