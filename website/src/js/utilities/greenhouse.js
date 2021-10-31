window.addEventListener('load', () => {
	if (-1 !== window.location.search.indexOf('gh_jid=')) {
		const scrollY = window.scrollY
		const offsetTop = document.querySelector('#jobs').getBoundingClientRect().top
		window.scrollTo({
			left: 0,
			top: scrollY + offsetTop,
		})
		window.setTimeout(() => {
			window.scrollTo({
				left: 0,
				top: scrollY + offsetTop - 40,
			})
		}, 50)
	}
})
