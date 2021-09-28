---
title: 'The Tests Are Passing, Why Would I Read The Diff Again?'
image: 'https://blog-images.clickhouse.tech/en/2021/code-review/two-ducks.jpg'
date: '2021-04-14'
author: '[Alexander Kuzmenkov](https://github.com/akuzm)'
tags: ['code review', 'development']
---


Code review is one of the few software development techniques that are consistently found to reduce the incidence of defects. Why is it effective? This article offers some wild conjecture on this topic, complete with practical advice on getting the most out of your code review.


## Understanding Why Your Program Works

As software developers, we routinely have to reason about the behaviour of software. For example, to fix a bug, we start with a test case that exhibits the behavior in question, and then read the source code to see how this behavior arises. Often we find ourselves unable to understand anything, having to resort to forensic techniques such as using a debugger or interrogating the author of the code. This situation is far from ideal. After all, if we have trouble understanding our software, how can we be sure it works at all? No surprise that it doesn't.

The correct understanding is also important when modifying and extending software. A programmer must always have a precise mental model on what is going on in the program, how exactly it maps to the domain, and so on. If there are flaws in this model, the code they write won't match the domain and won't solve the problem correctly. Wrong understanding directly causes bugs.

How can we make our software easier to understand? It is often said that to see if you really understand something, you have to try explaining it to somebody. For example, as a science student taking an exam, you might be expected to give an explanation to some well-known observed effect, deriving it from the basic laws of this domain. In a similar way, if we are modeling some problem in software, we can start from domain knowledge and general programming knowledge, and build an argument as to why our model is applicable to the problem, why it is correct, has optimal performance and so on. This explanation takes the form of code comments, or, at a higher level, design documents.

If you have a habit of thoroughly commenting your code, you might have noticed that writing the comments is often much harder than writing the code itself. It also has an unpleasant side effect &mdash; at times, while writing a comment, it becomes increasingly clear to you that the code is incomprehensible and takes forever to explain, or maybe is downright wrong, and you have to rewrite it. This is exactly the major positive effect of writing the comments. It helps you find bugs and make the code more understandable, and you wouldn't have noticed these problems unless you tried to explain the code.

Understanding why your program works is inseparable from understanding why it fails, so it's no surprise that there is a similar process for the latter, called "rubber duck debugging". To debug a particularly nasty bug, you start explaining the program logic step by step to an imaginary partner or even to an inanimate object such as a yellow rubber duck. This process is often very effective, much in excess of what one would expect given the limited conversational abilities of rubber ducks. The underlying mechanism is probably the same as with comments &mdash; you start to understand your program better by just trying to explain it, and this lets you find bugs.

When working in a team, you even have a luxury of explaining your code to another developer who works on the same project. It's probably more entertaining than talking to a duck. More importantly, they are going to maintain the code you wrote, so better make sure that _they_ can understand it as well. A good formal occasion for explaining how your code works is the code review process. Let's see how you can get the most out of it, in terms of making your code understandable.

## Reviewing Others Code

Code review is often framed as a gatekeeping process, where each contribution is vetted by maintainers to ensure that it is in line with project direction, has acceptable quality, meets the coding guidelines and so on. This perspective might seem natural when dealing with external contributions, but makes less sense if you apply it to internal ones. After all, our fellow maintainers have perfect understanding of project goals and guidelines, probably they are more talented and experienced than us, and can be trusted to produce the best solution possible. How can an additional review be helpful?

A less-obvious, but very important, part of reviewing the code is just seeing whether it can be understood by another person. It is helpful regardless of the administrative roles and programming proficiency of the parties. What should you do as a reviewer if ease of understanding is your main priority?

You probably don't need to be concerned with trivia such as code style. There are automated tools for that. You might find some bugs, but this is probably a side effect. Your main task is making sense of the code.

Start with checking the high-level description of the problem that the pull request is trying to solve. Read the description of the bug it fixes, or the docs for the feature it adds. For bigger features, there is normally a design document that describes the overall implementation without getting too deep into the code details. After you understand the problem, start reading the code. Does it make sense to you? You shouldn't try too hard to understand it. Imagine that you are tired and under time pressure. If you feel you have to make a lot of effort to understand the code, ask the author for clarifications. As you talk, you might discover that the code is not correct, or it may be rewritten in a more straightforward way, or it needs more comments.

<img src="https://blog-images.clickhouse.tech/en/2021/code-review/hidden-items.png"/>

After you get the answers, don't forget to update the code and the comments to reflect them. Don't just stop after getting it explained to you personally. If you had a question as a reviewer, chances are that other people will also have this question later, but there might be nobody around to ask. They will have to resort to `git blame` and re-reading the entire pull request or several of them. Code archaeology is sometimes fun, but it's the last thing you want to do when you are investigating an urgent bug. All the answers should be on the surface.

Working with the author, you should ensure that the code is mostly obvious to anyone with basic domain and programming knowledge, and all non-obvious parts are clearly explained.

### Preparing Your Code For Review

As an author, you can also do some things to make your code easier to understand for the reviewer.

First of all, if you are implementing a major feature, it probably needs a round of design review before you even start writing code. Skipping a design review and jumping right into the code review can be a major source of frustration, because it might turn out that even the problem you are solving was formulated incorrectly, and all your work has to be thrown away. Of course, this is not prevented completely by design review, either. Programming is an iterative, exploratory activity, and in complex cases you only begin to grasp the problem after implementing a first solution, which you then realize is incorrect and has to be thrown away.

When preparing your code for review, your major objective is to make your problem and its solution clear to the reviewer. A good tool for this is code comments. Any sizable piece of logic should have an introductory comment describing its general purpose and outlining the implementation. This description can reference similar features, explain the difference to them, explain how it interfaces with other subsystems. A good place to put this general description is a function that serves as a main entry point for the feature, or other form of its public interface, or the most significant class, or the file containing the implementation, and so on.

Drilling down to each block of code, you should be able to explain what it does, why it does that, why this way and not another. If there are several ways of doing the thing, why did you choose this one? Of course, for some code these things follow from the more general comments and don't have to be restated. The mechanics of data manipulation should be apparent from the code itself. If you find yourself explaining a particular feature of the language, it's probably best not to use it.

Pay special attention to making the data structures apparent in the code, and their meaning and invariants well commented. The choice of data structures ultimately determines which algorithms you can apply, and sets the limits of performance, which is another reason why we should care about it as ClickHouse developers. 

When explaining the code, it is important to give your reader enough context, so that they can understand you without a deep investigation of the surrounding systems and obscure test cases. Give pointers to all the things that might be relevant to the task. If you know some corner cases which your code has to handle, describe them in enough detail so that they can be reproduced. If there is a relevant standard or a design document, reference it, or even quote it inline. If you're relying on some invariant in other system, mention it. It is good practice to add programmatic checks that mirror your comments, when it is easy to do so. Your comment about an invariant should be accompanied by an assertion, and an important scenario should be reproduced by a test case.

Don't worry about being too verbose. There is often not enough comments, but almost never too much of them.

## Common Concerns about Code Comments

It is common to hear objections to the idea of commenting the code, so let's discuss a couple of usual ones.

### Self-documenting Code

You can often see a perplexing idea that the source code can somehow be "self-documenting", or that the comments are a "code smell", and their presence indicates that the code is badly written. I have trouble imagining how this belief can be compatible with any experience in maintaining sufficiently complex and large software, over the years, in collaboration with others. The code and the comments describe different parts of the solution. The code describes the data structures and their transformations, but it cannot convey meaning. The names in the code serve as pointers that map the data and its transformations to the domain concepts, but they are schematic and lack nuance. It is not so difficult to write code that makes it easy to understand what's going on in terms of data manipulation. What it takes is mostly moderation, that is, stopping yourself from being too clever. For most code, it is easy to see what it does, but why? Why this way and not that way? Why is it correct? Why this fast path here helps? Why did you choose this data layout? How is this invariant guaranteed? And so on. This might be not so evident for a developer who is working alone on a short-lived project, because they have all the necessary context in their head. But when they have to work with other people (or even with themselves from past and future), or in an unfamiliar area, the importance of non-code, higher-level context becomes painfully clear. The idea that we should, or even can, somehow encode comments such as [this one](https://github.com/ClickHouse/ClickHouse/blob/26d5db32ae5c9f54b8825e2eca1f077a3b17c84a/src/Storages/MergeTree/KeyCondition.cpp#L1312-L1347) into names or control flow is just absurd.

### Obsolete Comments

The comments can't be checked by the compiler or the tests, so there is no automated way to make sure that they are up to date with the rest of the comments and the code. The possibility of comments gradually getting incorrect is sometimes used as an argument against having any comments at all.

This problem is not exclusive to the comments &mdash; the code also can and does become obsolete. Simple cases such as dead code can be detected by static analysis or studying the test coverage of code. More complex cases can only be found by proofreading, such as maintaining an invariant that is not important anymore, or preparing some data that is not needed.

While an obsolete comment can lead to a mistake, the same applies, perhaps more strongly, to the lack of comments. When you need some higher-level knowledge about the code, but it is not written down, you are forced to perform an entire investigation from first principles to understand what's going on, and this is error-prone. Even an obsolete comment likely gives a better starting point than nothing. Moreover, in a code base that makes an active use of the comments, they tend to be mostly correct. This is because the developers rely on comments, read and write them, pay attention to them during code review. The comments are routinely changed along with changing the code, and the outdated comments are soon noticed and fixed. This does require some habit. A lone comment in a vast desert of impenetrable self-documenting code is not going to fare well.


## Conclusion

Code review makes your software better, and a significant part of this probably comes from trying to understand what your software actually does. By paying attention specifically to this aspect of code review, you can make it even more efficient. You'll have less bugs, and your code will be easier to maintain &mdash; and what else could we ask for as software developers?


_2021-04-13 [Alexander Kuzmenkov](https://github.com/akuzm). Title photo by [Nikita Mikhaylov](https://github.com/nikitamikhaylov)_

_P.S. This text contains the personal opinions of the author, and is not an authoritative manual for ClickHouse maintainers._
