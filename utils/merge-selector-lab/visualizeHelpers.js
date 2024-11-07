export function valueToColor(value, min, max, saturation = 70, lightness = 50, hueMin = 120, hueMax = 360)
{
    // Ensure value is within the min/max range
    if (value < min) value = min;
    if (value > max) value = max;

    // Normalize the value to range from 0 to 1
    const normalizedValue = min == max ? 0 : (value - min) / (max - min);

    // Calculate hue based on the normalized value
    const hue = hueMin + normalizedValue * (hueMax - hueMin);

    // Return the HSL color string
    return `hsl(${hue}, ${saturation}%, ${lightness}%)`;
}

// Custom tick format function for displaying bytes with dynamic scaling (KB, MB, GB)
export function formatBytesWithUnit(bytes)
{
    if (bytes >= Math.pow(1024, 3))
        return (bytes / Math.pow(1024, 3)).toFixed(1) + ' GB';
    else if (bytes >= Math.pow(1024, 2))
        return (bytes / Math.pow(1024, 2)).toFixed(1) + ' MB';
    else if (bytes >= 1024)
        return (bytes / 1024).toFixed(1) + ' KB';
    else
        return bytes + ' B';
}

// Dynamically determine appropriate tick step and unit based on the max value
export function determineTickStep(maxValue)
{
    let step;
    let unit;

    if (maxValue >= Math.pow(1024, 3)) // For GB range
        unit = Math.pow(1024, 3); // 1 GB
    else if (maxValue >= Math.pow(1024, 2)) // For MB range
        unit = Math.pow(1024, 2); // 1 MB
    else if (maxValue >= 1024) // For KB range
        unit = 1024; // 1 KB
    else
        unit = 1; // Bytes range

    // Determine appropriate tick step based on the range (1, 2, 5, 10, 20, 50, 100, 200...)
    if (maxValue / unit > 500)
        step = 100 * unit;
    else if (maxValue / unit > 200)
        step = 50 * unit;
    else if (maxValue / unit > 100)
        step = 20 * unit;
    else if (maxValue / unit > 50)
        step = 10 * unit;
    else if (maxValue / unit > 20)
        step = 5 * unit;
    else if (maxValue / unit > 10)
        step = 2 * unit;
    else
        step = 1 * unit;

    return step;
}
