stacklesslib.base
=================

.. automodule:: stacklesslib.base
	:members:
	:undoc-members:
	:exclude-members: time, atomic

	.. autofunction:: atomic()

		Returns a context managar which increments the current tasklet's `atomic` property for the duration.
		This means that it will not be interrupted by pre-emptive scheduling of tasklets, or by
		another thread.

	.. autofunction:: time() -> time in seconds

		The most appropriate timing function for computing wall-clock time on the platform.

