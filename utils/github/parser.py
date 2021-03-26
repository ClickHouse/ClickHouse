# -*- coding: utf-8 -*-

class Description:
    '''Parsed description representation
    '''
    MAP_CATEGORY_TO_LABEL = {
        'New Feature': 'pr-feature',
        'Bug Fix': 'pr-bugfix',
        'Improvement': 'pr-improvement',
        'Performance Improvement': 'pr-performance',
        # 'Backward Incompatible Change': doesn't match anything
        'Build/Testing/Packaging Improvement': 'pr-build',
        'Non-significant (changelog entry is not needed)': 'pr-non-significant',
        'Non-significant (changelog entry is not required)': 'pr-non-significant',
        'Non-significant': 'pr-non-significant',
        'Documentation (changelog entry is not required)': 'pr-documentation',
        # 'Other': doesn't match anything
    }

    def __init__(self, pull_request):
        self.label_name = str()
        self.legal = False

        self._parse(pull_request['bodyText'])

    def _parse(self, text):
        lines = text.splitlines()
        next_category = False
        category = str()

        for line in lines:
            stripped = line.strip()

            if not stripped:
                continue

            if next_category:
                category = stripped
                next_category = False

            if stripped == 'I hereby agree to the terms of the CLA available at: https://yandex.ru/legal/cla/?lang=en':
                self.legal = True

            category_headers = (
                'Category (leave one):',
                'Changelog category (leave one):',
                'Changelog category:',
                'Category:'
            )

            if stripped in category_headers:
                next_category = True

        if category in Description.MAP_CATEGORY_TO_LABEL:
            self.label_name = Description.MAP_CATEGORY_TO_LABEL[category]
        else:
            if not category:
                print('Cannot find category in pr description')
            else:
                print('Unknown category: ' + category)
