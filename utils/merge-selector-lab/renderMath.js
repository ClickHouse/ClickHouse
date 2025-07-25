let katexLoaded = false;
let katexCallbacks = []; // Array to store multiple callbacks

export function renderMath(inputString)
{
    // Load KaTeX from CDN only on the first call
    if (!katexLoaded)
    {
        const katexScript = document.createElement('script');
        katexScript.src = 'https://cdn.jsdelivr.net/npm/katex@0.15.2/dist/katex.min.js';
        katexScript.defer = true;
        document.head.appendChild(katexScript);
        window.katexScript = katexScript;

        const katexStyle = document.createElement('link');
        katexStyle.href = 'https://cdn.jsdelivr.net/npm/katex@0.15.2/dist/katex.min.css';
        katexStyle.rel = 'stylesheet';
        document.head.appendChild(katexStyle);

        katexLoaded = true;
        katexScript.onload = () => {
            // Run all stored callbacks
            katexCallbacks.forEach((callback) => callback());
            katexCallbacks = []; // Clear callbacks after running them
        };
    }

    // Create a container for the rendered DOM element
    const container = document.createElement('div');

    // Wait for KaTeX to load if it hasn't yet
    const renderContent = () =>
    {
        if (window.katex)
        {
            try
            {
                // Use KaTeX to parse and render the entire content
                const htmlContent = inputString.replace(/(\$\$.*?\$\$|\$.*?\$)/g, (match) =>
                {
                    const isBlock = match.startsWith('$$') && match.endsWith('$$');
                    const mathContent = isBlock ? match.slice(2, -2) : match.slice(1, -1);
                    try
                    {
                        return katex.renderToString(mathContent, { displayMode: isBlock });
                    } catch (e)
                    {
                        console.error('Failed to render formula:', e);
                        return match; // Return original content if rendering fails
                    }
                });
                container.innerHTML = htmlContent;
            } catch (e)
            {
                console.error('Failed to render content:', e);
            }
        }
        else
        {
            // Store callback if KaTeX hasn't loaded yet
            katexCallbacks.push(renderContent);
        }
    };

    renderContent();
    return container;
}

// Example usage:
// const input = "Here is some math: $E = mc^2$ and a block formula: $$a^2 + b^2 = c^2$$";
// document.body.appendChild(renderMath(input));