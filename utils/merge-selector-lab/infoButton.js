import { renderMath } from './renderMath.js';

// UMD builds: they define the `Popper` and `tippy` globals (tippy needs `Popper`, so it goes second).
import './vendor/popper.min.js';
import './vendor/tippy-bundle.umd.min.js';
const tippy = globalThis.tippy;

export function infoButton(svgContainer, x, y, content)
{
    // Import CSS once
    let css_href = new URL('./vendor/tippy-light.css', import.meta.url).href;
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