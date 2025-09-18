# Data Vault 2.0 modeling

Further work is needed on this solution. Hubs are created for teh core entities User, User Registration, Play Session, User Payment Detail
Each of these Hubs should have associated Hub Satellites with them capture contextual details.

Insight: 68% of payments are made through mobile methods

TODO:

Plan should also be a Hub Satellite although it has fairly static data with Plans and their costs

Link Satellites could be put in place for User Registration <-> User Payment Detail

Reference Data setup for:
User Play Session -> Status Code
User Play Session -> Channel Code

Further denormalization could happen at a Business Vault type level, where HubSatellites have more reference data embedded and more data brought together in (i.e. User Registration data could be part of User Hub Satellite)

Credit card details should not be stored but rather only last 4 digits or other obsufucation 

Further unit tests need to be added and working. Ideally integration test with teh core classes that test the output, along with refactoring to centralize hash key egneration and other parts while using dependency injection to test the reusable components.