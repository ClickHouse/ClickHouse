const $allElements = document.querySelectorAll('[equalize-heights]')
const groupedElements = {}

$allElements.forEach(($el) => {
	const group = $el.getAttribute('equalize-heights')
	groupedElements[group] = groupedElements[group] || []
	groupedElements[group].push($el)
})

function resizeElements() {
	Object.entries(groupedElements).forEach(([group, $elements]) => {
		$elements.forEach(($el) => {
			const styles = window.getComputedStyle($el)
			if ('none' === styles.getPropertyValue('display')) {
				$el.style.display = 'block'
			}
			$el.style.minHeight = 'auto'
		})
		const minHeight = $elements.reduce((max, $el) => {
			if ($el.offsetHeight > max) {
				max = $el.offsetHeight
			}
			return max
		}, 0)
		$elements.forEach(($el) => {
			$el.style.display = null
			$el.style.minHeight = `${minHeight}px`
		})
	})
}
	
window.addEventListener('resize', resizeElements)
window.addEventListener('orientationchange', resizeElements)

resizeElements()
