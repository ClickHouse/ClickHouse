import { renderMath } from './renderMath.js';

export function infoButton(svgContainer, x, y, content)
{
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