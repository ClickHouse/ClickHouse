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
        return parseInt(sizeInput);
    }
}
