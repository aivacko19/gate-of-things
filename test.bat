@echo off

for %%a in (access_control^
			administrator^
			device_registry^
			gateway^
			logger^
			message_delivery^
			oauth_interface^
			request_router^
			subscription_manager^
	) do (
	cd %%a
	@echo on
	echo.
	echo ################## %%a service ##################
	echo.
	rem python -m unittest discover
	pytest
	@echo off
	cd ..
)

