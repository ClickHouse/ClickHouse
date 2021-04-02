// Generator of protobuf delimited messages used in the protobuf IO tests
// tests/queries/0_stateless/00825_protobuf_format*

#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>
#include <google/protobuf/util/delimited_message_util.h>
#include "00825_protobuf_format.pb.h"
#include "00825_protobuf_format_syntax2.pb.h"


void writeInsertDataQueryForInputTest(std::stringstream & delimited_messages, const std::string & table_name, const std::string & format_schema, std::ostream & out)    // STYLE_CHECK_ALLOW_STD_STRING_STREAM
{
    out << "echo -ne '";
    std::string bytes = delimited_messages.str();
    delimited_messages.str("");
    for (const char c : bytes)
    {
        char buf[5];
        sprintf(buf, "\\x%02x", static_cast<unsigned char>(c));
        out << buf;
    }
    out << "' | $CLICKHOUSE_CLIENT --query=\"INSERT INTO " << table_name << " FORMAT Protobuf"
           " SETTINGS format_schema = '$CURDIR/"
        << format_schema << "'\"" << std::endl;
}

void writeInsertDataQueriesForInputTest(std::ostream & out)
{
    std::stringstream ss;       // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    {
        Person person;
        person.set_uuid("a7522158-3d41-4b77-ad69-6c598ee55c49");
        person.set_name("Ivan");
        person.set_surname("Petrov");
        person.set_gender(Gender::male);
        person.set_birthdate(4015); // 1980-12-29
        person.set_photo("png");
        person.set_phonenumber("+74951234567");
        person.set_isonline(true);
        person.set_visittime(1546703100); // 2019-01-05 18:45:00
        person.set_age(38);
        person.set_zodiacsign(ZodiacSign::capricorn);
        person.add_songs("Yesterday");
        person.add_songs("Flowers");
        person.add_color(255);
        person.add_color(0);
        person.add_color(0);
        person.set_hometown("Moscow");
        person.add_location(55.753215);
        person.add_location(37.622504);
        person.set_pi(3.14);
        person.set_lotterywin(214.10);
        person.set_someratio(0.1);
        person.set_temperature(5.8);
        person.set_randombignumber(17060000000);
        auto* mu = person.add_measureunits();
        mu->set_unit("meter");
        mu->set_coef(1);
        mu = person.add_measureunits();
        mu->set_unit("centimeter");
        mu->set_coef(0.01);
        mu = person.add_measureunits();
        mu->set_unit("kilometer");
        mu->set_coef(1000);
        person.mutable_nestiness()->mutable_a()->mutable_b()->mutable_c()->set_d(500);
        person.mutable_nestiness()->mutable_a()->mutable_b()->mutable_c()->add_e(501);
        person.mutable_nestiness()->mutable_a()->mutable_b()->mutable_c()->add_e(502);
        google::protobuf::util::SerializeDelimitedToOstream(person, &ss);
    }

    {
        Person person;
        person.set_uuid("c694ad8a-f714-4ea3-907d-fd54fb25d9b5");
        person.set_name("Natalia");
        person.set_surname("Sokolova");
        person.set_gender(Gender::female);
        person.set_birthdate(8102); // 1992-03-08
        person.set_photo("jpg");
        person.set_isonline(false);
        person.set_age(26);
        person.set_zodiacsign(ZodiacSign::pisces);
        person.add_color(100);
        person.add_color(200);
        person.add_color(50);
        person.set_hometown("Plymouth");
        person.add_location(50.403724);
        person.add_location(-4.142123);
        person.set_pi(3.14159);
        person.set_someratio(0.007);
        person.set_temperature(5.4);
        person.set_randombignumber(-20000000000000);
        google::protobuf::util::SerializeDelimitedToOstream(person, &ss);
    }

    {
        Person person;
        person.set_uuid("a7da1aa6-f425-4789-8947-b034786ed374");
        person.set_name("Vasily");
        person.set_surname("Sidorov");
        person.set_gender(Gender::male);
        person.set_birthdate(9339); // 1995-07-28
        person.set_photo("bmp");
        person.set_phonenumber("+442012345678");
        person.set_isonline(true);
        person.set_visittime(1546117200); // 2018-12-30 00:00:00
        person.set_age(23);
        person.set_zodiacsign(ZodiacSign::leo);
        person.add_songs("Sunny");
        person.add_color(250);
        person.add_color(244);
        person.add_color(10);
        person.set_hometown("Murmansk");
        person.add_location(68.970682);
        person.add_location(33.074981);
        person.set_pi(3.14159265358979);
        person.set_lotterywin(100000000000);
        person.set_someratio(800);
        person.set_temperature(-3.2);
        person.set_randombignumber(154400000);
        auto* mu = person.add_measureunits();
        mu->set_unit("pound");
        mu->set_coef(16);
        person.mutable_nestiness()->mutable_a()->mutable_b()->mutable_c()->set_d(503);
        google::protobuf::util::SerializeDelimitedToOstream(person, &ss);
    }

    writeInsertDataQueryForInputTest(ss, "in_persons_00825", "00825_protobuf_format:Person", out);

    {
        AltPerson person;
        person.add_location(42);
        person.add_location(-88);
        person.set_pi(3.141);
        person.set_uuid("20fcd95a-332d-41db-a9ec-161f644d059c");
        person.set_name("Frida");
        person.set_gender(AltPerson::female);
        person.set_zodiacsign(1122); // sagittarius
        person.set_birthdate(3267); // 1978-12-12
        person.set_age("40");
        person.set_isonline(OnlineStatus::offline);
        person.set_someratio(0.5);
        person.set_visittime(1363005000); // 2013-03-11 16:30:00
        person.set_randombignumber(8010000009);
        person.add_color(110);
        person.add_color(210);
        person.add_color(74);
        person.set_lotterywin(311);
        person.set_surname("Ermakova");
        person.set_phonenumber(3124555929);
        person.set_temperature(10);
        person.add_measureunits_unit("KB");
        person.add_measureunits_coef(1024);
        person.add_measureunits_unit("MB");
        person.add_measureunits_coef(1048576);
        person.set_nestiness_a_b_c_d(700);
        person.add_nestiness_a_b_c_e(701);
        google::protobuf::util::SerializeDelimitedToOstream(person, &ss);
    }

    {
        AltPerson person;
        person.add_location(26);
        person.add_location(-80);
        person.set_pi(3.1416);
        person.set_uuid("7cfa6856-a54a-4786-b8e5-745159d52278");
        person.set_name("Isolde");
        person.set_gender(AltPerson::female);
        person.set_zodiacsign(120); // aquarius
        person.set_birthdate(6248); // 1987-02-09
        person.set_age("32");
        person.set_isonline(OnlineStatus::online);
        person.set_someratio(4.5);
        person.set_randombignumber(-11111111111111);
        person.add_color(255);
        person.add_color(0);
        person.add_color(255);
        person.set_surname("Lavrova");
        person.set_temperature(25);
        person.set_newfieldstr("abc");
        person.set_newfieldbool(true);
        person.add_newfieldint(44);
        person.add_measureunits_unit("Byte");
        person.add_measureunits_coef(8);
        person.add_measureunits_unit("Bit");
        person.add_measureunits_coef(1);
        person.mutable_newmessage()->set_z(91);
        person.set_nestiness_a_b_c_d(702);
        google::protobuf::util::SerializeDelimitedToOstream(person, &ss);
    }

    writeInsertDataQueryForInputTest(ss, "in_persons_00825", "00825_protobuf_format:AltPerson", out);

    {
        StrPerson person;
        person.set_uuid("aa0e5a06-cab2-4034-a6a2-48e82b91664e");
        person.set_name("Leonid");
        person.set_surname("Kirillov");
        person.set_gender("male");
        person.set_birthdate("1983-06-24");
        person.set_phonenumber("+74950275864");
        person.set_isonline("1");
        person.set_visittime("2019-02-04 09:45:00");
        person.set_age("35");
        person.set_zodiacsign("cancer");
        person.add_songs("7 rings");
        person.add_songs("Eastside");
        person.add_songs("Last Hurrah");
        person.add_color("0");
        person.add_color("0");
        person.add_color("255");
        person.set_hometown("San Diego");
        person.add_location("32.823943");
        person.add_location("-117.081327");
        person.set_pi("3.1415927");
        person.set_lotterywin("15000000");
        person.set_someratio("186.75");
        person.set_temperature("-2.1");
        person.set_randombignumber("20659829331");
        person.mutable_measureunits()->add_unit("minute");
        person.mutable_measureunits()->add_coef("60");
        person.mutable_measureunits()->add_unit("hour");
        person.mutable_measureunits()->add_coef("3600");
        person.mutable_nestiness_a()->mutable_b_c()->add_e("1800");
        google::protobuf::util::SerializeDelimitedToOstream(person, &ss);
    }

    writeInsertDataQueryForInputTest(ss, "in_persons_00825", "00825_protobuf_format:StrPerson", out);

    {
        Syntax2Person person;
        person.set_uuid("3faee064-c4f7-4d34-b6f3-8d81c2b6a15d");
        person.set_name("Nick");
        person.set_surname("Kolesnikov");
        person.set_gender(Syntax2Person::male);
        person.set_birthdate(10586); // 1998-12-26
        person.set_photo("bmp");
        person.set_phonenumber("412-687-5007");
        person.set_isonline(true);
        person.set_visittime(1542596399); // 2018-11-19 05:59:59
        person.set_age(20);
        person.set_zodiacsign(Syntax2Person::capricorn);
        person.add_songs("Havana");
        person.add_color(128);
        person.add_color(0);
        person.add_color(128);
        person.set_hometown("Pittsburgh");
        person.add_location(40.517193);
        person.add_location(-79.949452);
        person.set_pi(3.1415926535898);
        person.set_lotterywin(50000000000);
        person.set_someratio(780);
        person.set_temperature(18.3);
        person.set_randombignumber(195500007);
        person.mutable_measureunits()->add_unit("ounce");
        person.mutable_measureunits()->add_coef(28.35);
        person.mutable_measureunits()->add_unit("carat");
        person.mutable_measureunits()->add_coef(0.2);
        person.mutable_measureunits()->add_unit("gram");
        person.mutable_measureunits()->add_coef(1);
        person.mutable_nestiness()->mutable_a()->mutable_b()->mutable_c()->set_d(9494);
        google::protobuf::util::SerializeDelimitedToOstream(person, &ss);
    }

    writeInsertDataQueryForInputTest(ss, "in_persons_00825", "00825_protobuf_format_syntax2:Syntax2Person", out);

    {
        NumberAndSquare ns;
        ns.set_number(2);
        ns.set_square(4);
        google::protobuf::util::SerializeDelimitedToOstream(ns, &ss);
    }

    {
        NumberAndSquare ns;
        ns.set_number(0);
        ns.set_square(0);
        google::protobuf::util::SerializeDelimitedToOstream(ns, &ss);
    }

    {
        NumberAndSquare ns;
        ns.set_number(3);
        ns.set_square(9);
        google::protobuf::util::SerializeDelimitedToOstream(ns, &ss);
    }

    writeInsertDataQueryForInputTest(ss, "in_squares_00825", "00825_protobuf_format:NumberAndSquare", out);
}


void writeReferenceForOutputTest(std::ostream & out)
{
    {
        Person person;
        person.set_uuid("a7522158-3d41-4b77-ad69-6c598ee55c49");
        person.set_name("Ivan");
        person.set_surname("Petrov");
        person.set_gender(Gender::male);
        person.set_birthdate(4015); // 1980-12-29
        person.set_photo("png");
        person.set_phonenumber(std::string("+74951234567\0", 13)); // Converted from FixedString(13)
        person.set_isonline(true);
        person.set_visittime(1546703100); // 2019-01-05 18:45:00
        person.set_age(38);
        person.set_zodiacsign(ZodiacSign::capricorn);
        person.add_songs("Yesterday");
        person.add_songs("Flowers");
        person.add_color(255);
        person.add_color(0);
        person.add_color(0);
        person.set_hometown("Moscow");
        person.add_location(55.753215);
        person.add_location(37.622504);
        person.set_pi(3.14);
        person.set_lotterywin(214.10);
        person.set_someratio(0.1);
        person.set_temperature(5.8);
        person.set_randombignumber(17060000000);
        auto* mu = person.add_measureunits();
        mu->set_unit("meter");
        mu->set_coef(1);
        mu = person.add_measureunits();
        mu->set_unit("centimeter");
        mu->set_coef(0.01);
        mu = person.add_measureunits();
        mu->set_unit("kilometer");
        mu->set_coef(1000);
        person.mutable_nestiness()->mutable_a()->mutable_b()->mutable_c()->set_d(500);
        person.mutable_nestiness()->mutable_a()->mutable_b()->mutable_c()->add_e(501);
        person.mutable_nestiness()->mutable_a()->mutable_b()->mutable_c()->add_e(502);
        google::protobuf::util::SerializeDelimitedToOstream(person, &out);
    }

    {
        Person person;
        person.set_uuid("c694ad8a-f714-4ea3-907d-fd54fb25d9b5");
        person.set_name("Natalia");
        person.set_surname("Sokolova");
        person.set_gender(Gender::female);
        person.set_birthdate(8102); // 1992-03-08
        person.set_photo("jpg");
        person.set_isonline(false);
        person.set_age(26);
        person.set_zodiacsign(ZodiacSign::pisces);
        person.add_color(100);
        person.add_color(200);
        person.add_color(50);
        person.set_hometown("Plymouth");
        person.add_location(50.403724);
        person.add_location(-4.142123);
        person.set_pi(3.14159);
        person.set_someratio(0.007);
        person.set_temperature(5.4);
        person.set_randombignumber(-20000000000000);
        google::protobuf::util::SerializeDelimitedToOstream(person, &out);
    }

    {
        Person person;
        person.set_uuid("a7da1aa6-f425-4789-8947-b034786ed374");
        person.set_name("Vasily");
        person.set_surname("Sidorov");
        person.set_gender(Gender::male);
        person.set_birthdate(9339); // 1995-07-28
        person.set_photo("bmp");
        person.set_phonenumber("+442012345678");
        person.set_isonline(true);
        person.set_visittime(1546117200); // 2018-12-30 00:00:00
        person.set_age(23);
        person.set_zodiacsign(ZodiacSign::leo);
        person.add_songs("Sunny");
        person.add_color(250);
        person.add_color(244);
        person.add_color(10);
        person.set_hometown("Murmansk");
        person.add_location(68.970682);
        person.add_location(33.074981);
        person.set_pi(3.14159265358979);
        person.set_lotterywin(100000000000);
        person.set_someratio(800);
        person.set_temperature(-3.2);
        person.set_randombignumber(154400000);
        auto* mu = person.add_measureunits();
        mu->set_unit("pound");
        mu->set_coef(16);
        person.mutable_nestiness()->mutable_a()->mutable_b()->mutable_c()->set_d(503);
        google::protobuf::util::SerializeDelimitedToOstream(person, &out);
    }

    out << "ALTERNATIVE->" << std::endl;

    {
        AltPerson person;
        person.add_location(55);
        person.add_location(37);
        person.set_pi(3.14);
        person.set_uuid("a7522158-3d41-4b77-ad69-6c598ee55c49");
        person.set_name("Ivan");
        person.set_gender(AltPerson::male);
        person.set_zodiacsign(1222); // capricorn
        person.set_birthdate(4015); // 1980-12-29
        person.set_age("38");
        person.set_isonline(OnlineStatus::online);
        person.set_someratio(0.100000001490116119384765625); // 0.1 converted from float to double
        person.set_visittime(1546703100); // 2019-01-05 18:45:00
        person.set_randombignumber(17060000000);
        person.add_color(255);
        person.add_color(0);
        person.add_color(0);
        person.set_lotterywin(214);
        person.set_surname("Petrov");
        person.set_phonenumber(+74951234567);
        person.set_temperature(5);
        person.add_measureunits_unit("meter");
        person.add_measureunits_coef(1);
        person.add_measureunits_unit("centimeter");
        person.add_measureunits_coef(0.01);
        person.add_measureunits_unit("kilometer");
        person.add_measureunits_coef(1000);
        person.set_nestiness_a_b_c_d(500);
        person.add_nestiness_a_b_c_e(501);
        person.add_nestiness_a_b_c_e(502);
        google::protobuf::util::SerializeDelimitedToOstream(person, &out);
    }

    {
        AltPerson person;
        person.add_location(50);
        person.add_location(-4);
        person.set_pi(3.14159);
        person.set_uuid("c694ad8a-f714-4ea3-907d-fd54fb25d9b5");
        person.set_name("Natalia");
        person.set_gender(AltPerson::female);
        person.set_zodiacsign(219); // pisces
        person.set_birthdate(8102); // 1992-03-08
        person.set_age("26");
        person.set_isonline(OnlineStatus::offline);
        person.set_someratio(0.007000000216066837310791015625); // 0.007 converted from float to double
        person.set_randombignumber(-20000000000000);
        person.add_color(100);
        person.add_color(200);
        person.add_color(50);
        person.set_surname("Sokolova");
        person.set_temperature(5);
        google::protobuf::util::SerializeDelimitedToOstream(person, &out);
    }

    {
        AltPerson person;
        person.add_location(68);
        person.add_location(33);
        person.set_pi(3.1415926535897);
        person.set_uuid("a7da1aa6-f425-4789-8947-b034786ed374");
        person.set_name("Vasily");
        person.set_gender(AltPerson::male);
        person.set_zodiacsign(723); // leo
        person.set_birthdate(9339); // 1995-07-28
        person.set_age("23");
        person.set_isonline(OnlineStatus::online);
        person.set_someratio(800);
        person.set_visittime(1546117200); // 2018-12-30 00:00:00
        person.set_randombignumber(154400000);
        person.add_color(250);
        person.add_color(244);
        person.add_color(10);
        person.set_lotterywin(100000000000);
        person.set_surname("Sidorov");
        person.set_phonenumber(+442012345678);
        person.set_temperature(-3);
        person.add_measureunits_unit("pound");
        person.add_measureunits_coef(16);
        person.set_nestiness_a_b_c_d(503);
        google::protobuf::util::SerializeDelimitedToOstream(person, &out);
    }

    out << "STRINGS->" << std::endl;

    {
        StrPerson person;
        person.set_uuid("a7522158-3d41-4b77-ad69-6c598ee55c49");
        person.set_name("Ivan");
        person.set_surname("Petrov");
        person.set_gender("male");
        person.set_birthdate("1980-12-29");
        person.set_phonenumber(std::string("+74951234567\0", 13)); // Converted from FixedString(13)
        person.set_isonline("1");
        person.set_visittime("2019-01-05 18:45:00");
        person.set_age("38");
        person.set_zodiacsign("capricorn");
        person.add_songs("Yesterday");
        person.add_songs("Flowers");
        person.add_color("255");
        person.add_color("0");
        person.add_color("0");
        person.set_hometown("Moscow");
        person.add_location("55.753215");
        person.add_location("37.622504");
        person.set_pi("3.14");
        person.set_lotterywin("214.10");
        person.set_someratio("0.1");
        person.set_temperature("5.8");
        person.set_randombignumber("17060000000");
        person.mutable_measureunits()->add_unit("meter");
        person.mutable_measureunits()->add_coef("1");
        person.mutable_measureunits()->add_unit("centimeter");
        person.mutable_measureunits()->add_coef("0.01");
        person.mutable_measureunits()->add_unit("kilometer");
        person.mutable_measureunits()->add_coef("1000");
        person.mutable_nestiness_a()->mutable_b_c()->set_d("500");
        person.mutable_nestiness_a()->mutable_b_c()->add_e("501");
        person.mutable_nestiness_a()->mutable_b_c()->add_e("502");
        google::protobuf::util::SerializeDelimitedToOstream(person, &out);
    }

    {
        StrPerson person;
        person.set_uuid("c694ad8a-f714-4ea3-907d-fd54fb25d9b5");
        person.set_name("Natalia");
        person.set_surname("Sokolova");
        person.set_gender("female");
        person.set_birthdate("1992-03-08");
        person.set_isonline("0");
        person.set_age("26");
        person.set_zodiacsign("pisces");
        person.add_color("100");
        person.add_color("200");
        person.add_color("50");
        person.set_hometown("Plymouth");
        person.add_location("50.403724");
        person.add_location("-4.142123");
        person.set_pi("3.14159");
        person.set_someratio("0.007");
        person.set_temperature("5.4");
        person.set_randombignumber("-20000000000000");
        google::protobuf::util::SerializeDelimitedToOstream(person, &out);
    }

    {
        StrPerson person;
        person.set_uuid("a7da1aa6-f425-4789-8947-b034786ed374");
        person.set_name("Vasily");
        person.set_surname("Sidorov");
        person.set_gender("male");
        person.set_birthdate("1995-07-28");
        person.set_phonenumber("+442012345678");
        person.set_isonline("1");
        person.set_visittime("2018-12-30 00:00:00");
        person.set_age("23");
        person.set_zodiacsign("leo");
        person.add_songs("Sunny");
        person.add_color("250");
        person.add_color("244");
        person.add_color("10");
        person.set_hometown("Murmansk");
        person.add_location("68.970682");
        person.add_location("33.074981");
        person.set_pi("3.14159265358979");
        person.set_lotterywin("100000000000.00");
        person.set_someratio("800");
        person.set_temperature("-3.2");
        person.set_randombignumber("154400000");
        person.mutable_measureunits()->add_unit("pound");
        person.mutable_measureunits()->add_coef("16");
        person.mutable_nestiness_a()->mutable_b_c()->set_d("503");
        google::protobuf::util::SerializeDelimitedToOstream(person, &out);
    }

    out << "SYNTAX2->" << std::endl;

    {
        Syntax2Person person;
        person.set_uuid("a7522158-3d41-4b77-ad69-6c598ee55c49");
        person.set_name("Ivan");
        person.set_surname("Petrov");
        person.set_gender(Syntax2Person::male);
        person.set_birthdate(4015); // 1980-12-29
        person.set_photo("png");
        person.set_phonenumber(std::string("+74951234567\0", 13)); // Converted from FixedString(13)
        person.set_isonline(true);
        person.set_visittime(1546703100); // 2019-01-05 18:45:00
        person.set_age(38);
        person.set_zodiacsign(Syntax2Person::capricorn);
        person.add_songs("Yesterday");
        person.add_songs("Flowers");
        person.add_color(255);
        person.add_color(0);
        person.add_color(0);
        person.set_hometown("Moscow");
        person.add_location(55.753215);
        person.add_location(37.622504);
        person.set_pi(3.14);
        person.set_lotterywin(214.10);
        person.set_someratio(0.1);
        person.set_temperature(5.8);
        person.set_randombignumber(17060000000);
        person.mutable_measureunits()->add_unit("meter");
        person.mutable_measureunits()->add_coef(1);
        person.mutable_measureunits()->add_unit("centimeter");
        person.mutable_measureunits()->add_coef(0.01);
        person.mutable_measureunits()->add_unit("kilometer");
        person.mutable_measureunits()->add_coef(1000);
        person.mutable_nestiness()->mutable_a()->mutable_b()->mutable_c()->set_d(500);
        person.mutable_nestiness()->mutable_a()->mutable_b()->mutable_c()->add_e(501);
        person.mutable_nestiness()->mutable_a()->mutable_b()->mutable_c()->add_e(502);
        google::protobuf::util::SerializeDelimitedToOstream(person, &out);
    }

    {
        Syntax2Person person;
        person.set_uuid("c694ad8a-f714-4ea3-907d-fd54fb25d9b5");
        person.set_name("Natalia");
        person.set_surname("Sokolova");
        person.set_gender(Syntax2Person::female);
        person.set_birthdate(8102); // 1992-03-08
        person.set_photo("jpg");
        person.set_isonline(false);
        person.set_age(26);
        person.set_zodiacsign(Syntax2Person::pisces);
        person.add_color(100);
        person.add_color(200);
        person.add_color(50);
        person.set_hometown("Plymouth");
        person.add_location(50.403724);
        person.add_location(-4.142123);
        person.set_pi(3.14159);
        person.set_someratio(0.007);
        person.set_temperature(5.4);
        person.set_randombignumber(-20000000000000);
        google::protobuf::util::SerializeDelimitedToOstream(person, &out);
    }

    {
        Syntax2Person person;
        person.set_uuid("a7da1aa6-f425-4789-8947-b034786ed374");
        person.set_name("Vasily");
        person.set_surname("Sidorov");
        person.set_gender(Syntax2Person::male);
        person.set_birthdate(9339); // 1995-07-28
        person.set_photo("bmp");
        person.set_phonenumber("+442012345678");
        person.set_isonline(true);
        person.set_visittime(1546117200); // 2018-12-30 00:00:00
        person.set_age(23);
        person.set_zodiacsign(Syntax2Person::leo);
        person.add_songs("Sunny");
        person.add_color(250);
        person.add_color(244);
        person.add_color(10);
        person.set_hometown("Murmansk");
        person.add_location(68.970682);
        person.add_location(33.074981);
        person.set_pi(3.14159265358979);
        person.set_lotterywin(100000000000);
        person.set_someratio(800);
        person.set_temperature(-3.2);
        person.set_randombignumber(154400000);
        person.mutable_measureunits()->add_unit("pound");
        person.mutable_measureunits()->add_coef(16);
        person.mutable_nestiness()->mutable_a()->mutable_b()->mutable_c()->set_d(503);
        google::protobuf::util::SerializeDelimitedToOstream(person, &out);
    }

    out << "SQUARES->" << std::endl;

    {
        NumberAndSquare ns;
        ns.set_number(0);
        ns.set_square(0);
        google::protobuf::util::SerializeDelimitedToOstream(ns, &out);
    }

    {
        NumberAndSquare ns;
        ns.set_number(2);
        ns.set_square(4);
        google::protobuf::util::SerializeDelimitedToOstream(ns, &out);
    }

    {
        NumberAndSquare ns;
        ns.set_number(3);
        ns.set_square(9);
        google::protobuf::util::SerializeDelimitedToOstream(ns, &out);
    }
}


void parseCommandLine(int argc, char ** argv, std::string & output_dir)
{
    namespace po = boost::program_options;
    po::options_description desc;
    output_dir = OUTPUT_DIR;
    desc.add_options()
        ("help,h", "Show help")
        ("directory,d", po::value<std::string>(&output_dir),
         "Set the output directory. By default it's " OUTPUT_DIR);
    po::parsed_options parsed = po::command_line_parser(argc, argv).options(desc).run();
    po::variables_map vm;
    po::store(parsed, vm);
    po::notify(vm);
    if (!output_dir.empty())
        return;

    // Show help.
    std::cout << "This utility generates delimited messages for tests checking protobuf IO support." << std::endl;
    std::cout << desc;
    std::cout << "Example:" << std::endl;
    std::cout << argv[0] << " -g OUTPUT_REFERENCE" << std::endl;
    std::exit(0);
}

void writeFile(const std::string & filepath, void (*fn)(std::ostream &))
{
    std::cout << "Writing '" << filepath << "' ... ";
    std::fstream out(filepath, std::fstream::out | std::fstream::trunc);
    fn(out);
    std::cout << "done." << std::endl;
}

int main(int argc, char ** argv)
{
    std::string output_dir;
    parseCommandLine(argc, argv, output_dir);
    writeFile(output_dir + "/00825_protobuf_format_input.insh", writeInsertDataQueriesForInputTest);
    writeFile(output_dir + "/00825_protobuf_format_output.reference", writeReferenceForOutputTest);
    return 0;
}
