class CaseStudyCard {
	
	constructor($el) {
		this.onOpen = this.onOpen.bind(this)
		this.onToggle = this.onToggle.bind(this)
		this.$el = $el
		this.$el.addEventListener('click', this.onOpen)
		this.$el.querySelector('.case-study-card-toggle').addEventListener('click', this.onToggle)
		this.open = false
	}

	onOpen() {
		this.open = true
		this.$el.classList.toggle('is-open', this.open)
		this.$el.classList.toggle('is-closing', !this.open)
		this.closeOthers()
	}

	onToggle(event) {
		event.stopPropagation()
		this.open = !this.$el.classList.contains('is-open')
		this.$el.classList.toggle('is-open', this.open)
		this.$el.classList.toggle('is-closing', !this.open)
		this.closeOthers()
	}

	closeOthers() {
		if (this.open) {
			document.querySelectorAll('.case-study-card').forEach(($el) => {
				if (!$el.isSameNode(this.$el)) {
					$el.classList.toggle('is-closing', $el.classList.contains('is-open'))
					$el.classList.toggle('is-open', false)
				}
			})
		}
	}

}

document.querySelectorAll('.case-study-card').forEach(($el) => new CaseStudyCard($el))
