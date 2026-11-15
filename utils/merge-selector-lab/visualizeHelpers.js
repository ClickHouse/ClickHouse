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

// Dynamically determine appropriate tick step and unit based on the max value
export function determineTickStep(maxValue, base = 1000)
{
    let unit;
    if (maxValue >= 4 * Math.pow(base, 5))
        unit = Math.pow(base, 5); // 1 P
    else if (maxValue >= 4 * Math.pow(base, 4))
        unit = Math.pow(base, 4); // 1 T
    else if (maxValue >= 4 * Math.pow(base, 3))
        unit = Math.pow(base, 3); // 1 G
    else if (maxValue >= 4 * Math.pow(base, 2))
        unit = Math.pow(base, 2); // 1 M
    else if (maxValue >= 4 * base)
        unit = base; // 1 K
    else
        unit = 1;

    let step;
    if (maxValue / unit > 2000)
        step = 500 * unit;
    else if (maxValue / unit > 1000)
        step = 200 * unit;
    else if (maxValue / unit > 500)
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
