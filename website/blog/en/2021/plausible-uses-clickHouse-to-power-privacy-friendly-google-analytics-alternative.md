---
title: 'Plausible Analytics uses ClickHouse to power their privacy-friendly Google Analytics alternative'
image: 'https://blog-images.clickhouse.com/en/2021/plausible-uses-clickHouse-to-power-privacy-friendly-google-analytics-alternative/featured-cropped.jpg'
date: '2021-12-08'
author: 'Elissa Weve'
tags: ['company']
---

Plausible Analytics is a lightweight, open source web analytics tool that has quickly gained popularity as the privacy-friendly alternative to Google Analytics. By using Plausible Analytics, customers keep 100% ownership of their website data and protect the privacy of their visitors since there are no cookies and it is fully compliant with GDPR.

Since its launch in April 2019, the analytics platform has scaled to service 5000+ paying subscribers. With an annual recurring revenue of half a million dollars, Plausible Analytics currently tracks 28,000 different websites and more than 1 billion page views per month.

Marko Saric, co-founder at Plausible Analytics, said to handle this increase in volume, it became clear early on that the original architecture using Postgres to store analytics data could not handle the platform’s future growth. 

“We knew that if we’re going to go anywhere in the future we needed something better,” Saric said.

## “Best technical decision we ever made”

Through word of mouth, the Plausible team received the recommendation to try ClickHouse. They quickly noticed significant improvements in the loading speed of their dashboards. With Postgres, their dashboards were taking 5 seconds to load; Now with ClickHouse, it took less than a second. 

Plausible co-founder Uku Täht said the team also tried a couple of other solutions, but “Clickhouse came on top in terms of both performance and features that we would make use of,” he said.

“Plausible Analytics is a lightweight product, so it is important that everything loads quickly—the dashboard, segmentation of the data, and all the cool stuff in the background. Customers don’t know what we’re doing in the background, but they know that they want a fast experience,” Saric added.

“Plausible Analytics is a lightweight product, so it is important that everything loads quickly—the dashboard, segmentation of the data, and all the cool stuff in the background. Customers don’t know what we’re doing in the background, but they know that they want a fast experience,” Saric added. Using ClickHouse, Plausible Analytics is able to serve even its largest customers with ease, including the biggest customer, with 150 million pages per month. “This would not have been possible previously, it would have crashed everything, it would not have been able to load.,” Saric said. “There would have been no chance we could have had that kind of customer.”

According to Täht, switching to ClickHouse was the best technical decision their team ever made. “Clickhouse is amazingly efficient, not just in terms of compute power needed but also the time that it saves us. It's very easy to work with Clickhouse. It does exactly what we need and it does it exceptionally well. It's one of those technologies that feels really simple to use but also has a rich feature set.”

“I don’t think we would be able to be where we are today without ClickHouse,” Saric said. “Without switching from Postgres, Plausible would not have all this growth and new customers.”

## About Plausible

Plausible Analytics is an open-source project dedicated to making web analytics more privacy-friendly. Our mission is to reduce corporate surveillance by providing an alternative web analytics tool which doesn’t come from the AdTech world.

Visit [plausible.io](https://plausible.io/) for more information or to start a free trial.


