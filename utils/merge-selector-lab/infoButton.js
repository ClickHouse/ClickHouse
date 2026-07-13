import { renderMath } from './renderMath.js';

import tippy from 'https://cdn.skypack.dev/tippy.js@6';
import 'https://cdn.skypack.dev/@popperjs/core';

export function infoButton(svgContainer, x, y, content)
{
    // Import CSS once
    let css_href = 'https://unpkg.com/tippy.js@6/themes/light.css';
    if (!document.querySelector(`link[href="${css_href}"]`)) {
        // Create and append link tag to load CSS if not already present
        const link = document.createElement('link');
        link.rel = 'stylesheet';
        link.href = css_href;
        document.head.appendChild(link);
    }

    const infoGroup = svgContainer.append("g")
        .attr("class", "chart-description")
        .attr("transform", `translate(${x}, ${y})`);

    infoGroup.append("text")
        .attr("text-anchor", "middle")
        .attr("y", 4)
        .attr("fill", "white")
        .style("font-size", "16px")
        .text("ℹ");

    return tippy(infoGroup.node(), {
        content: renderMath(content),
        allowHTML: true,
        placement: 'bottom',
        theme: 'light',
        trigger: 'click',
        arrow: true,
        maxWidth: '600px',
    });
}